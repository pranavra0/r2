use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;

use crate::Canonical;
use crate::data::{Digest, Ref, Symbol, Term, Value};
use crate::eval::{Continuation, EvalError, EvalResult, Reified, RuntimeValue, Yielded, eval};
use crate::host::{
    HostEffectCaching, HostEffectPolicy, HostEffectProvenance, HostError, HostHandler,
    HostTraceEvent, materialize_cached_effect_outputs, verify_cached_effect_inputs,
};
use crate::store::{CachedThunk, MemoryStore, ObjectStore, StoreError, Stored};
use crate::thunk::{self, ThunkError};

const DEFAULT_CACHE_LIMIT: usize = 10_000;
const RECORD_GET_OP: &str = "record.get";
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub enum RuntimeError {
    Eval(EvalError),
    Store(StoreError),
    Host(HostError),
    Thunk(ThunkError),
    UnhandledEffect { op: Symbol },
    CachedOutputMaterialization { message: String },
    CacheInvalidation { message: String },
    Builtin { op: Symbol, message: String },
    Cancelled,
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Eval(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
            Self::Host(error) => error.fmt(f),
            Self::Thunk(error) => error.fmt(f),
            Self::UnhandledEffect { op } => write!(f, "unhandled effect {op}"),
            Self::CachedOutputMaterialization { message } => {
                write!(f, "cached output materialization failed: {message}")
            }
            Self::CacheInvalidation { message } => {
                write!(
                    f,
                    "cached thunk invalidated due to changed inputs: {message}"
                )
            }
            Self::Builtin { op, message } => write!(f, "{op}: {message}"),
            Self::Cancelled => f.write_str("cancelled"),
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<EvalError> for RuntimeError {
    fn from(value: EvalError) -> Self {
        Self::Eval(value)
    }
}

impl From<StoreError> for RuntimeError {
    fn from(value: StoreError) -> Self {
        Self::Store(value)
    }
}

impl From<HostError> for RuntimeError {
    fn from(value: HostError) -> Self {
        Self::Host(value)
    }
}

impl From<ThunkError> for RuntimeError {
    fn from(value: ThunkError) -> Self {
        Self::Thunk(value)
    }
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn check(&self) -> Result<(), RuntimeError> {
        if self.is_cancelled() {
            Err(RuntimeError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeStoredKind {
    Term,
    Value,
}

impl fmt::Display for RuntimeStoredKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Term => f.write_str("term"),
            Self::Value => f.write_str("value"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeValueKind {
    Data,
    Closure,
    Continuation,
    Ref,
}

impl RuntimeValueKind {
    fn of(value: &RuntimeValue) -> Self {
        match value {
            RuntimeValue::Data(_) => Self::Data,
            RuntimeValue::Closure(_) | RuntimeValue::RecursiveClosure(_) => Self::Closure,
            RuntimeValue::Continuation(_) => Self::Continuation,
            RuntimeValue::Ref(_) => Self::Ref,
        }
    }
}

impl fmt::Display for RuntimeValueKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data => f.write_str("data"),
            Self::Closure => f.write_str("closure"),
            Self::Continuation => f.write_str("continuation"),
            Self::Ref => f.write_str("ref"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeTraceEvent {
    EvalStart {
        closed: bool,
        digest: Option<Digest>,
    },
    MemoHit {
        digest: Digest,
    },
    MemoStore {
        digest: Digest,
    },
    RefLoad {
        hash: Digest,
        kind: RuntimeStoredKind,
    },
    Yield {
        op: Symbol,
    },
    BuiltinHandle {
        op: Symbol,
    },
    HostHandle {
        op: Symbol,
        policy: HostEffectPolicy,
    },
    HostEvent {
        op: Symbol,
        phase: Symbol,
        fields: BTreeMap<Symbol, Value>,
    },
    UnhandledEffect {
        op: Symbol,
    },
    ThunkForce {
        key: Digest,
    },
    ThunkForceAll {
        frontier_id: u64,
        count: usize,
    },
    TaskStart {
        frontier_id: u64,
        task_id: u64,
    },
    TaskEnd {
        frontier_id: u64,
        task_id: u64,
    },
    ThunkCacheHit {
        key: Digest,
    },
    ThunkCacheStore {
        key: Digest,
    },
    ThunkCacheInvalidated {
        key: Digest,
    },
    ThunkCacheBypass {
        key: Digest,
        op: Option<Symbol>,
        policy: Option<HostEffectPolicy>,
    },
    Persisted {
        hash: Digest,
        kind: RuntimeStoredKind,
    },
    RunComplete {
        kind: RuntimeValueKind,
    },
}

impl fmt::Display for RuntimeTraceEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EvalStart {
                closed: true,
                digest: Some(digest),
            } => write!(f, "eval start: closed {digest}"),
            Self::EvalStart {
                closed: true,
                digest: None,
            } => f.write_str("eval start: closed"),
            Self::EvalStart { closed: false, .. } => f.write_str("eval start: open"),
            Self::MemoHit { digest } => write!(f, "memo hit: {digest}"),
            Self::MemoStore { digest } => write!(f, "memo store: {digest}"),
            Self::RefLoad { hash, kind } => write!(f, "load {kind}: {hash}"),
            Self::Yield { op } => write!(f, "yield: {op}"),
            Self::BuiltinHandle { op } => write!(f, "builtin handle: {op}"),
            Self::HostHandle { op, policy } => write!(f, "host handle: {op} [{policy}]"),
            Self::HostEvent { op, phase, fields } => {
                write!(f, "host event: {op} {phase} {fields:?}")
            }
            Self::UnhandledEffect { op } => write!(f, "unhandled effect: {op}"),
            Self::ThunkForce { key } => write!(f, "thunk force: {key}"),
            Self::ThunkForceAll { frontier_id, count } => {
                write!(f, "thunk force_all[{frontier_id}]: {count}")
            }
            Self::TaskStart {
                frontier_id,
                task_id,
            } => {
                write!(f, "task {task_id} start: frontier {frontier_id}")
            }
            Self::TaskEnd {
                frontier_id,
                task_id,
            } => {
                write!(f, "task {task_id} end: frontier {frontier_id}")
            }
            Self::ThunkCacheHit { key } => write!(f, "thunk cache hit: {key}"),
            Self::ThunkCacheStore { key } => write!(f, "thunk cache store: {key}"),
            Self::ThunkCacheInvalidated { key } => {
                write!(f, "thunk cache invalidated: {key}")
            }
            Self::ThunkCacheBypass {
                key,
                op: Some(op),
                policy: Some(policy),
            } => {
                write!(f, "thunk cache bypass: {key} due to {policy} effect {op}")
            }
            Self::ThunkCacheBypass { key, op: None, .. } => {
                write!(f, "thunk cache bypass: {key}")
            }
            Self::ThunkCacheBypass {
                key,
                op: Some(op),
                policy: None,
            } => write!(f, "thunk cache bypass: {key} due to effect {op}"),
            Self::Persisted { hash, kind } => write!(f, "persist {kind}: {hash}"),
            Self::RunComplete { kind } => write!(f, "run complete: {kind}"),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeTrace {
    events: Vec<RuntimeTraceEvent>,
}

impl RuntimeTrace {
    pub fn events(&self) -> &[RuntimeTraceEvent] {
        &self.events
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn step_count(&self) -> usize {
        self.events.len()
    }

    pub fn summary(&self) -> RuntimeTraceSummary {
        let mut summary = RuntimeTraceSummary {
            total_events: self.events.len(),
            ..RuntimeTraceSummary::default()
        };

        for event in &self.events {
            match event {
                RuntimeTraceEvent::EvalStart { .. } => summary.eval_starts += 1,
                RuntimeTraceEvent::MemoHit { .. } => summary.memo_hits += 1,
                RuntimeTraceEvent::MemoStore { .. } => summary.memo_stores += 1,
                RuntimeTraceEvent::RefLoad { .. } => summary.ref_loads += 1,
                RuntimeTraceEvent::Yield { .. } => summary.yields += 1,
                RuntimeTraceEvent::BuiltinHandle { .. } => summary.builtin_handles += 1,
                RuntimeTraceEvent::HostHandle { policy, .. } => {
                    summary.host_handles += 1;
                    match (policy.caching(), policy.provenance()) {
                        (HostEffectCaching::Deny, HostEffectProvenance::Ambient) => {
                            summary.volatile_host_handles += 1;
                        }
                        (HostEffectCaching::Allow, HostEffectProvenance::Ambient) => {
                            summary.stable_host_handles += 1;
                        }
                        (HostEffectCaching::Deny, HostEffectProvenance::Declared) => {
                            summary.declared_host_handles += 1;
                        }
                        (HostEffectCaching::Allow, HostEffectProvenance::Declared) => {
                            summary.hermetic_host_handles += 1;
                        }
                    }
                }
                RuntimeTraceEvent::HostEvent { op, phase, .. } => {
                    if op.as_str() == "service.supervise" {
                        match phase.as_str() {
                            "spawn" => summary.service_spawns += 1,
                            "exit" => summary.service_exits += 1,
                            "restart" => summary.service_restarts += 1,
                            "stop" => summary.service_stops += 1,
                            _ => {}
                        }
                    }
                }
                RuntimeTraceEvent::UnhandledEffect { .. } => summary.unhandled_effects += 1,
                RuntimeTraceEvent::ThunkForce { .. } => summary.thunk_forces += 1,
                RuntimeTraceEvent::ThunkForceAll { .. } => summary.thunk_force_all += 1,
                RuntimeTraceEvent::TaskStart { .. } => summary.task_starts += 1,
                RuntimeTraceEvent::TaskEnd { .. } => summary.task_ends += 1,
                RuntimeTraceEvent::ThunkCacheHit { .. } => summary.thunk_cache_hits += 1,
                RuntimeTraceEvent::ThunkCacheStore { .. } => summary.thunk_cache_stores += 1,
                RuntimeTraceEvent::ThunkCacheInvalidated { .. } => {
                    summary.thunk_cache_invalidations += 1;
                }
                RuntimeTraceEvent::ThunkCacheBypass { .. } => summary.thunk_cache_bypasses += 1,
                RuntimeTraceEvent::Persisted { kind, .. } => {
                    summary.persisted += 1;
                    match kind {
                        RuntimeStoredKind::Term => summary.persisted_terms += 1,
                        RuntimeStoredKind::Value => summary.persisted_values += 1,
                    }
                }
                RuntimeTraceEvent::RunComplete { .. } => summary.run_completions += 1,
            }
        }

        summary
    }

    fn push(&mut self, event: RuntimeTraceEvent) {
        self.events.push(event);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RuntimeTraceSummary {
    pub total_events: usize,
    pub eval_starts: usize,
    pub memo_hits: usize,
    pub memo_stores: usize,
    pub ref_loads: usize,
    pub yields: usize,
    pub builtin_handles: usize,
    pub host_handles: usize,
    pub stable_host_handles: usize,
    pub volatile_host_handles: usize,
    pub declared_host_handles: usize,
    pub hermetic_host_handles: usize,
    pub service_spawns: usize,
    pub service_exits: usize,
    pub service_restarts: usize,
    pub service_stops: usize,
    pub unhandled_effects: usize,
    pub thunk_forces: usize,
    pub thunk_force_all: usize,
    pub task_starts: usize,
    pub task_ends: usize,
    pub thunk_cache_hits: usize,
    pub thunk_cache_stores: usize,
    pub thunk_cache_invalidations: usize,
    pub thunk_cache_bypasses: usize,
    pub persisted: usize,
    pub persisted_terms: usize,
    pub persisted_values: usize,
    pub run_completions: usize,
}

#[derive(Clone, Debug)]
pub struct TracedRun {
    pub value: RuntimeValue,
    pub trace: RuntimeTrace,
}

trait TraceSink {
    fn record(&mut self, event: RuntimeTraceEvent);
}

#[derive(Default)]
struct NullTrace;

impl TraceSink for NullTrace {
    fn record(&mut self, _event: RuntimeTraceEvent) {}
}

impl TraceSink for RuntimeTrace {
    fn record(&mut self, event: RuntimeTraceEvent) {
        self.push(event);
    }
}

impl TraceSink for Arc<Mutex<RuntimeTrace>> {
    fn record(&mut self, event: RuntimeTraceEvent) {
        self.lock()
            .expect("runtime trace mutex should not be poisoned")
            .push(event);
    }
}

#[derive(Clone, Debug)]
struct RunOutcome {
    value: RuntimeValue,
    cacheable: bool,
    uncacheable_effect: Option<Symbol>,
    uncacheable_policy: Option<HostEffectPolicy>,
}

#[derive(Clone, Debug)]
struct ForcedThunk {
    value: RuntimeValue,
    cacheable: bool,
    uncacheable_effect: Option<Symbol>,
    uncacheable_policy: Option<HostEffectPolicy>,
}

impl ForcedThunk {
    fn cacheable(value: RuntimeValue) -> Self {
        Self {
            value,
            cacheable: true,
            uncacheable_effect: None,
            uncacheable_policy: None,
        }
    }
}

#[derive(Clone, Debug)]
struct BuiltinOutcome {
    result: EvalResult,
    cacheable: bool,
    uncacheable_effect: Option<Symbol>,
    uncacheable_policy: Option<HostEffectPolicy>,
}

#[derive(Clone, Debug)]
struct ForceAllOutcome {
    values: Vec<Value>,
    cacheable: bool,
    uncacheable_effect: Option<Symbol>,
    uncacheable_policy: Option<HostEffectPolicy>,
}

struct BranchResult<S> {
    result: Result<ForcedThunk, RuntimeError>,
    trace: RuntimeTrace,
    store: S,
}

struct SharedHost<'a> {
    host: Arc<Mutex<&'a mut (dyn HostHandler + Send + 'a)>>,
}

impl<'a> SharedHost<'a> {
    fn new(host: &'a mut (dyn HostHandler + Send + 'a)) -> Self {
        Self {
            host: Arc::new(Mutex::new(host)),
        }
    }
}

impl Clone for SharedHost<'_> {
    fn clone(&self) -> Self {
        Self {
            host: Arc::clone(&self.host),
        }
    }
}

impl HostHandler for SharedHost<'_> {
    fn handle(
        &mut self,
        op: &Symbol,
        args: Vec<RuntimeValue>,
        continuation: Continuation,
    ) -> Result<Option<EvalResult>, HostError> {
        let concurrent_handler = self
            .host
            .lock()
            .expect("shared host mutex should not be poisoned")
            .concurrent_handler(op);
        if let Some(handler) = concurrent_handler {
            return handler(args, continuation).map(Some);
        }

        self.host
            .lock()
            .expect("shared host mutex should not be poisoned")
            .handle(op, args, continuation)
    }

    fn effect_policy(&self, op: &Symbol) -> HostEffectPolicy {
        self.host
            .lock()
            .expect("shared host mutex should not be poisoned")
            .effect_policy(op)
    }

    fn drain_trace_events(&mut self) -> Vec<HostTraceEvent> {
        self.host
            .lock()
            .expect("shared host mutex should not be poisoned")
            .drain_trace_events()
    }
}

#[derive(Clone, Debug)]
pub struct Runtime<S = MemoryStore> {
    store: S,
    state: Arc<Mutex<RuntimeState>>,
}

#[derive(Clone, Debug)]
struct RuntimeState {
    memo: BTreeMap<Digest, Reified>,
    thunk_cache: BTreeMap<Digest, Reified>,
    memo_order: Vec<Digest>,
    thunk_cache_order: Vec<Digest>,
    max_memo_entries: usize,
    max_thunk_cache_entries: usize,
}

impl Runtime<MemoryStore> {
    pub fn new() -> Self {
        Self::with_store(MemoryStore::new())
    }
}

impl Default for Runtime<MemoryStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Runtime<S> {
    pub fn with_store(store: S) -> Self {
        Self {
            store,
            state: Arc::new(Mutex::new(RuntimeState {
                memo: BTreeMap::new(),
                thunk_cache: BTreeMap::new(),
                memo_order: Vec::new(),
                thunk_cache_order: Vec::new(),
                max_memo_entries: DEFAULT_CACHE_LIMIT,
                max_thunk_cache_entries: DEFAULT_CACHE_LIMIT,
            })),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn memo_len(&self) -> usize {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .memo
            .len()
    }

    pub fn thunk_cache_len(&self) -> usize {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .thunk_cache
            .len()
    }

    pub fn with_memo_limit(self, max_entries: usize) -> Self {
        {
            let mut state = self
                .state
                .lock()
                .expect("runtime state mutex should not be poisoned");
            state.max_memo_entries = max_entries;
            state.evict_memo_to_limit();
        }
        self
    }

    pub fn with_thunk_cache_limit(self, max_entries: usize) -> Self {
        {
            let mut state = self
                .state
                .lock()
                .expect("runtime state mutex should not be poisoned");
            state.max_thunk_cache_entries = max_entries;
            state.evict_thunk_cache_to_limit();
        }
        self
    }

    pub fn max_memo_entries(&self) -> usize {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .max_memo_entries
    }

    pub fn max_thunk_cache_entries(&self) -> usize {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .max_thunk_cache_entries
    }

    fn insert_memo(&mut self, digest: Digest, value: Reified) {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .insert_memo(digest, value);
    }

    fn get_memo(&mut self, digest: &Digest) -> Option<Reified> {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .get_memo(digest)
    }

    fn insert_thunk_cache(&mut self, digest: Digest, value: Reified) {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .insert_thunk_cache(digest, value);
    }

    fn get_thunk_cache(&mut self, digest: &Digest) -> Option<Reified> {
        self.state
            .lock()
            .expect("runtime state mutex should not be poisoned")
            .get_thunk_cache(digest)
    }
}

impl RuntimeState {
    fn insert_memo(&mut self, digest: Digest, value: Reified) {
        self.memo.insert(digest, value);
        touch_access_order(&mut self.memo_order, digest);
        self.evict_memo_to_limit();
    }

    fn get_memo(&mut self, digest: &Digest) -> Option<Reified> {
        let value = self.memo.get(digest).cloned();
        if value.is_some() {
            touch_access_order(&mut self.memo_order, *digest);
        }
        value
    }

    fn evict_memo_to_limit(&mut self) {
        while self.memo.len() > self.max_memo_entries {
            let Some(digest) = self.memo_order.first().copied() else {
                break;
            };
            self.memo_order.remove(0);
            self.memo.remove(&digest);
        }
    }

    fn insert_thunk_cache(&mut self, digest: Digest, value: Reified) {
        self.thunk_cache.insert(digest, value);
        touch_access_order(&mut self.thunk_cache_order, digest);
        self.evict_thunk_cache_to_limit();
    }

    fn get_thunk_cache(&mut self, digest: &Digest) -> Option<Reified> {
        let value = self.thunk_cache.get(digest).cloned();
        if value.is_some() {
            touch_access_order(&mut self.thunk_cache_order, *digest);
        }
        value
    }

    fn evict_thunk_cache_to_limit(&mut self) {
        while self.thunk_cache.len() > self.max_thunk_cache_entries {
            let Some(digest) = self.thunk_cache_order.first().copied() else {
                break;
            };
            self.thunk_cache_order.remove(0);
            self.thunk_cache.remove(&digest);
        }
    }
}

impl<S: ObjectStore> Runtime<S> {
    pub fn intern_term(&mut self, term: Term) -> Result<Ref, RuntimeError> {
        let mut trace = NullTrace;
        self.intern_term_with_trace(term, &mut trace)
    }

    pub fn intern_value(&mut self, value: Value) -> Result<Ref, RuntimeError> {
        let mut trace = NullTrace;
        self.intern_value_with_trace(value, &mut trace)
    }

    pub fn load(&self, reference: &Ref) -> Result<Stored, RuntimeError> {
        let mut trace = NullTrace;
        self.load_with_trace(reference, &mut trace)
    }

    pub fn eval(&mut self, term: Term) -> Result<EvalResult, RuntimeError> {
        let mut trace = NullTrace;
        self.eval_with_trace_sink(term, &mut trace)
    }

    fn eval_with_trace_sink<T: TraceSink>(
        &mut self,
        term: Term,
        trace: &mut T,
    ) -> Result<EvalResult, RuntimeError> {
        let digest = term.is_closed().then(|| term.digest());
        trace.record(RuntimeTraceEvent::EvalStart {
            closed: digest.is_some(),
            digest,
        });

        if let Some(reified) = digest.and_then(|digest| self.get_memo(&digest)) {
            trace.record(RuntimeTraceEvent::MemoHit {
                digest: digest.unwrap(),
            });
            return Ok(EvalResult::Done(reified.into_runtime()));
        }

        let result = eval(term.clone())?;

        if let (Some(digest), EvalResult::Done(value)) = (&digest, &result)
            && let Some(reified) = value.reify()
        {
            self.insert_memo(*digest, reified);
            trace.record(RuntimeTraceEvent::MemoStore { digest: *digest });
        }

        Ok(result)
    }

    pub fn eval_ref(&mut self, reference: &Ref) -> Result<EvalResult, RuntimeError> {
        let mut trace = NullTrace;
        self.eval_ref_with_trace_sink(reference, &mut trace)
    }

    fn eval_ref_with_trace_sink<T: TraceSink>(
        &mut self,
        reference: &Ref,
        trace: &mut T,
    ) -> Result<EvalResult, RuntimeError> {
        match self.load_with_trace(reference, trace)? {
            Stored::Term(term) => self.eval_with_trace_sink(term, trace),
            Stored::Value(value) => Ok(EvalResult::Done(RuntimeValue::Data(value))),
        }
    }

    pub fn run<H: HostHandler + Send>(
        &mut self,
        term: Term,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let token = CancellationToken::new();
        self.run_with_cancellation(term, host, &token)
    }

    pub fn run_with_cancellation<H: HostHandler + Send>(
        &mut self,
        term: Term,
        host: &mut H,
        cancellation: &CancellationToken,
    ) -> Result<RuntimeValue, RuntimeError> {
        let mut trace = NullTrace;
        Ok(self
            .run_with_trace_sink(term, host, &mut trace, cancellation)?
            .value)
    }

    pub fn run_with_trace<H: HostHandler + Send>(
        &mut self,
        term: Term,
        host: &mut H,
    ) -> Result<TracedRun, RuntimeError> {
        let token = CancellationToken::new();
        self.run_with_trace_and_cancellation(term, host, &token)
    }

    pub fn run_with_trace_and_cancellation<H: HostHandler + Send>(
        &mut self,
        term: Term,
        host: &mut H,
        cancellation: &CancellationToken,
    ) -> Result<TracedRun, RuntimeError> {
        let mut trace = RuntimeTrace::default();
        let value = self
            .run_with_trace_sink(term, host, &mut trace, cancellation)?
            .value;
        Ok(TracedRun { value, trace })
    }

    pub fn run_ref<H: HostHandler + Send>(
        &mut self,
        reference: &Ref,
        host: &mut H,
    ) -> Result<RuntimeValue, RuntimeError> {
        let token = CancellationToken::new();
        self.run_ref_with_cancellation(reference, host, &token)
    }

    pub fn run_ref_with_cancellation<H: HostHandler + Send>(
        &mut self,
        reference: &Ref,
        host: &mut H,
        cancellation: &CancellationToken,
    ) -> Result<RuntimeValue, RuntimeError> {
        let mut trace = NullTrace;
        Ok(self
            .run_ref_with_trace_sink(reference, host, &mut trace, cancellation)?
            .value)
    }

    pub fn run_ref_with_trace<H: HostHandler + Send>(
        &mut self,
        reference: &Ref,
        host: &mut H,
    ) -> Result<TracedRun, RuntimeError> {
        let token = CancellationToken::new();
        self.run_ref_with_trace_and_cancellation(reference, host, &token)
    }

    pub fn run_ref_with_trace_and_cancellation<H: HostHandler + Send>(
        &mut self,
        reference: &Ref,
        host: &mut H,
        cancellation: &CancellationToken,
    ) -> Result<TracedRun, RuntimeError> {
        let mut trace = RuntimeTrace::default();
        let value = self
            .run_ref_with_trace_sink(reference, host, &mut trace, cancellation)?
            .value;
        Ok(TracedRun { value, trace })
    }

    fn intern_term_with_trace<T: TraceSink>(
        &mut self,
        term: Term,
        trace: &mut T,
    ) -> Result<Ref, RuntimeError> {
        let reference = self
            .store
            .put(Stored::term(term))
            .map_err(RuntimeError::from)?;
        trace.record(RuntimeTraceEvent::Persisted {
            hash: reference.hash,
            kind: RuntimeStoredKind::Term,
        });
        Ok(reference)
    }

    fn intern_value_with_trace<T: TraceSink>(
        &mut self,
        value: Value,
        trace: &mut T,
    ) -> Result<Ref, RuntimeError> {
        let reference = self
            .store
            .put(Stored::value(value))
            .map_err(RuntimeError::from)?;
        trace.record(RuntimeTraceEvent::Persisted {
            hash: reference.hash,
            kind: RuntimeStoredKind::Value,
        });
        Ok(reference)
    }

    fn load_with_trace<T: TraceSink>(
        &self,
        reference: &Ref,
        trace: &mut T,
    ) -> Result<Stored, RuntimeError> {
        let stored = self.store.load(reference).map_err(RuntimeError::from)?;
        trace.record(RuntimeTraceEvent::RefLoad {
            hash: reference.hash,
            kind: stored.kind(),
        });
        Ok(stored)
    }

    fn run_with_trace_sink<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        term: Term,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<RunOutcome, RuntimeError> {
        cancellation.check()?;
        let result = self.eval_with_trace_sink(term, trace)?;
        self.drive_with_trace(result, host, trace, cancellation)
    }

    fn run_ref_with_trace_sink<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        reference: &Ref,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<RunOutcome, RuntimeError> {
        cancellation.check()?;
        let result = self.eval_ref_with_trace_sink(reference, trace)?;
        self.drive_with_trace(result, host, trace, cancellation)
    }

    fn drive_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        mut result: EvalResult,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<RunOutcome, RuntimeError> {
        let mut cacheable = true;
        let mut uncacheable_effect = None;
        let mut uncacheable_policy = None;

        loop {
            cancellation.check()?;
            match result {
                EvalResult::Done(value) => {
                    trace.record(RuntimeTraceEvent::RunComplete {
                        kind: RuntimeValueKind::of(&value),
                    });
                    return Ok(RunOutcome {
                        value,
                        cacheable,
                        uncacheable_effect,
                        uncacheable_policy,
                    });
                }
                EvalResult::Yielded(yielded) => {
                    trace.record(RuntimeTraceEvent::Yield {
                        op: yielded.op.clone(),
                    });
                    if let Some(builtin) =
                        self.handle_builtin_with_trace(yielded.clone(), host, trace, cancellation)?
                    {
                        if !builtin.cacheable {
                            cacheable = false;
                            if uncacheable_effect.is_none() {
                                uncacheable_effect = builtin.uncacheable_effect;
                                uncacheable_policy = builtin.uncacheable_policy;
                            }
                        }
                        trace.record(RuntimeTraceEvent::BuiltinHandle { op: yielded.op });
                        result = builtin.result;
                    } else if let Some(next) =
                        host.handle(&yielded.op, yielded.args, yielded.continuation)?
                    {
                        let policy = host.effect_policy(&yielded.op);
                        if !policy.allows_thunk_cache() {
                            cacheable = false;
                            if uncacheable_effect.is_none() {
                                uncacheable_effect = Some(yielded.op.clone());
                                uncacheable_policy = Some(policy);
                            }
                        }
                        trace.record(RuntimeTraceEvent::HostHandle {
                            op: yielded.op,
                            policy,
                        });
                        for event in host.drain_trace_events() {
                            trace.record(event.into());
                        }
                        result = next;
                    } else {
                        trace.record(RuntimeTraceEvent::UnhandledEffect {
                            op: yielded.op.clone(),
                        });
                        return Err(RuntimeError::UnhandledEffect { op: yielded.op });
                    }
                }
            }
        }
    }

    fn handle_builtin_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        yielded: Yielded,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<Option<BuiltinOutcome>, RuntimeError> {
        if yielded.op.as_str() == thunk::FORCE_OP {
            return Ok(Some(self.handle_thunk_force_with_trace(
                yielded,
                host,
                trace,
                cancellation,
            )?));
        }
        if yielded.op.as_str() == thunk::FORCE_ALL_OP {
            return Ok(Some(self.handle_thunk_force_all_with_trace(
                yielded,
                host,
                trace,
                cancellation,
            )?));
        }
        if yielded.op.as_str() == RECORD_GET_OP {
            return Ok(Some(handle_record_get(yielded)?));
        }

        Ok(None)
    }

    fn handle_thunk_force_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        yielded: Yielded,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<BuiltinOutcome, RuntimeError> {
        let forced = self.force_thunk_args_with_trace(yielded.args, host, trace, cancellation)?;
        let result = yielded
            .continuation
            .resume(forced.value)
            .map_err(RuntimeError::from)?;
        Ok(BuiltinOutcome {
            result,
            cacheable: forced.cacheable,
            uncacheable_effect: forced.uncacheable_effect,
            uncacheable_policy: forced.uncacheable_policy,
        })
    }

    fn handle_thunk_force_all_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        yielded: Yielded,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<BuiltinOutcome, RuntimeError> {
        let values =
            self.force_all_thunk_args_with_trace(yielded.args, host, trace, cancellation)?;
        let result = yielded
            .continuation
            .resume(RuntimeValue::Data(Value::List(values.values)))
            .map_err(RuntimeError::from)?;
        Ok(BuiltinOutcome {
            result,
            cacheable: values.cacheable,
            uncacheable_effect: values.uncacheable_effect,
            uncacheable_policy: values.uncacheable_policy,
        })
    }

    fn force_thunk_args_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        args: Vec<RuntimeValue>,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<ForcedThunk, RuntimeError> {
        let mut args = args.into_iter();
        let thunk = args.next().ok_or(ThunkError::WrongArgumentCount {
            expected: 1,
            found: 0,
        })?;

        if args.next().is_some() {
            return Err(ThunkError::WrongArgumentCount {
                expected: 1,
                found: 2,
            }
            .into());
        }

        self.force_thunk_with_trace(thunk, host, trace, cancellation)
    }

    fn force_all_thunk_args_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        args: Vec<RuntimeValue>,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<ForceAllOutcome, RuntimeError> {
        let frontier_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
        trace.record(RuntimeTraceEvent::ThunkForceAll {
            frontier_id,
            count: args.len(),
        });

        let thunks = args
            .iter()
            .map(reified_thunk_term)
            .collect::<Result<Vec<_>, _>>()?;
        if thunks.len() <= 1 {
            let mut values = Vec::with_capacity(thunks.len());
            let mut cacheable = true;
            let mut uncacheable_effect = None;
            let mut uncacheable_policy = None;
            for (key, term) in thunks {
                let forced =
                    self.force_reified_thunk_with_trace(key, term, host, trace, cancellation)?;
                if !forced.cacheable {
                    cacheable = false;
                    if uncacheable_effect.is_none() {
                        uncacheable_effect = forced.uncacheable_effect.clone();
                        uncacheable_policy = forced.uncacheable_policy;
                    }
                }
                match forced.value {
                    RuntimeValue::Data(value) => values.push(value),
                    value => return Err(EvalError::NonDataLiteralValue(value).into()),
                }
            }
            return Ok(ForceAllOutcome {
                values,
                cacheable,
                uncacheable_effect,
                uncacheable_policy,
            });
        }

        let shared_host = SharedHost::new(host);
        let state = Arc::clone(&self.state);
        let store = self.store.clone();
        let mut branch_results = thunks
            .into_par_iter()
            .map(|(key, term)| {
                let task_id = NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed);
                let mut branch_trace = RuntimeTrace::default();
                branch_trace.push(RuntimeTraceEvent::TaskStart {
                    frontier_id,
                    task_id,
                });
                let mut branch_runtime = Runtime {
                    store: store.clone(),
                    state: Arc::clone(&state),
                };
                let mut branch_host = shared_host.clone();
                let result = branch_runtime.force_reified_thunk_with_trace(
                    key,
                    term,
                    &mut branch_host,
                    &mut branch_trace,
                    cancellation,
                );
                branch_trace.push(RuntimeTraceEvent::TaskEnd {
                    frontier_id,
                    task_id,
                });
                BranchResult {
                    result,
                    trace: branch_trace,
                    store: branch_runtime.store,
                }
            })
            .collect::<Vec<_>>();

        let mut values = Vec::with_capacity(branch_results.len());
        let mut cacheable = true;
        let mut uncacheable_effect = None;
        let mut uncacheable_policy = None;
        for branch in branch_results.drain(..) {
            self.store.merge_from(branch.store)?;
            for event in branch.trace.events {
                trace.record(event);
            }
            let forced = branch.result?;
            if !forced.cacheable {
                cacheable = false;
                if uncacheable_effect.is_none() {
                    uncacheable_effect = forced.uncacheable_effect.clone();
                    uncacheable_policy = forced.uncacheable_policy;
                }
            }
            match forced.value {
                RuntimeValue::Data(value) => values.push(value),
                value => return Err(EvalError::NonDataLiteralValue(value).into()),
            }
        }

        Ok(ForceAllOutcome {
            values,
            cacheable,
            uncacheable_effect,
            uncacheable_policy,
        })
    }

    fn force_thunk_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        thunk_value: RuntimeValue,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<ForcedThunk, RuntimeError> {
        cancellation.check()?;
        let (key, term) = reified_thunk_term(&thunk_value)?;
        self.force_reified_thunk_with_trace(key, term, host, trace, cancellation)
    }

    fn force_reified_thunk_with_trace<H: HostHandler + Send, T: TraceSink>(
        &mut self,
        key: Digest,
        term: Term,
        host: &mut H,
        trace: &mut T,
        cancellation: &CancellationToken,
    ) -> Result<ForcedThunk, RuntimeError> {
        cancellation.check()?;
        trace.record(RuntimeTraceEvent::ThunkForce { key });

        if let Some(reified) = self.get_thunk_cache(&key) {
            if verify_reified_cached_effect_inputs(&reified)? {
                materialize_reified_cached_effect_outputs(&reified)?;
                trace.record(RuntimeTraceEvent::ThunkCacheHit { key });
                return Ok(ForcedThunk::cacheable(reified.into_runtime()));
            }
            trace.record(RuntimeTraceEvent::ThunkCacheInvalidated { key });
        }

        if let Some(reified) = self.load_cached_thunk_with_trace(&key, trace)? {
            if verify_reified_cached_effect_inputs(&reified)? {
                materialize_reified_cached_effect_outputs(&reified)?;
                self.insert_thunk_cache(key, reified.clone());
                trace.record(RuntimeTraceEvent::ThunkCacheHit { key });
                return Ok(ForcedThunk::cacheable(reified.into_runtime()));
            }
            trace.record(RuntimeTraceEvent::ThunkCacheInvalidated { key });
        }

        let outcome = self.run_with_trace_sink(term, host, trace, cancellation)?;

        if !outcome.cacheable {
            trace.record(RuntimeTraceEvent::ThunkCacheBypass {
                key,
                op: outcome.uncacheable_effect.clone(),
                policy: outcome.uncacheable_policy,
            });
            return Ok(ForcedThunk {
                value: outcome.value,
                cacheable: false,
                uncacheable_effect: outcome.uncacheable_effect,
                uncacheable_policy: outcome.uncacheable_policy,
            });
        }

        let reified = outcome
            .value
            .reify()
            .ok_or(ThunkError::UncacheableResult)?
            .clone();

        self.persist_reified_with_trace(key, &reified, trace)?;
        self.insert_thunk_cache(key, reified.clone());
        trace.record(RuntimeTraceEvent::ThunkCacheStore { key });
        Ok(ForcedThunk::cacheable(reified.into_runtime()))
    }

    fn load_cached_thunk_with_trace<T: TraceSink>(
        &mut self,
        key: &Digest,
        trace: &mut T,
    ) -> Result<Option<Reified>, RuntimeError> {
        let Some(cached) = self.store.get_cached_thunk(key)? else {
            return Ok(None);
        };

        let reified = match cached {
            CachedThunk::Value(reference) => match self.load_with_trace(&reference, trace)? {
                Stored::Value(value) => Reified::Value(value),
                Stored::Term(_) => return Err(ThunkError::InvalidCacheEntry.into()),
            },
            CachedThunk::Lambda(reference) => match self.load_with_trace(&reference, trace)? {
                Stored::Term(Term::Lambda(lambda)) => Reified::Lambda(lambda),
                Stored::Term(_) | Stored::Value(_) => {
                    return Err(ThunkError::InvalidCacheEntry.into());
                }
            },
            CachedThunk::Ref(reference) => Reified::Ref(reference),
        };

        Ok(Some(reified))
    }

    fn persist_reified_with_trace<T: TraceSink>(
        &mut self,
        key: Digest,
        reified: &Reified,
        trace: &mut T,
    ) -> Result<(), RuntimeError> {
        let cached = match reified {
            Reified::Value(value) => {
                CachedThunk::Value(self.intern_value_with_trace(value.clone(), trace)?)
            }
            Reified::Lambda(lambda) => CachedThunk::Lambda(
                self.intern_term_with_trace(Term::Lambda(lambda.clone()), trace)?,
            ),
            Reified::Ref(reference) => CachedThunk::Ref(reference.clone()),
        };

        self.store.put_cached_thunk(key, cached)?;

        Ok(())
    }
}

impl From<HostTraceEvent> for RuntimeTraceEvent {
    fn from(value: HostTraceEvent) -> Self {
        match value {
            HostTraceEvent::Lifecycle { op, phase, fields } => {
                Self::HostEvent { op, phase, fields }
            }
        }
    }
}

fn reified_thunk_term(thunk_value: &RuntimeValue) -> Result<(Digest, Term), ThunkError> {
    match thunk_value.reify() {
        Some(Reified::Lambda(lambda)) => {
            if lambda.parameters != 0 {
                return Err(ThunkError::WrongArity {
                    expected: 0,
                    found: usize::from(lambda.parameters),
                });
            }

            let term = Term::Apply {
                callee: Box::new(Term::Lambda(lambda)),
                args: Vec::new(),
            };
            let key = term.digest();
            Ok((key, term))
        }
        Some(Reified::Value(_)) | Some(Reified::Ref(_)) | None => Err(ThunkError::NotAThunk),
    }
}

fn materialize_reified_cached_effect_outputs(reified: &Reified) -> Result<(), RuntimeError> {
    let Reified::Value(value) = reified else {
        return Ok(());
    };

    materialize_cached_effect_outputs(value)
        .map_err(|message| RuntimeError::CachedOutputMaterialization { message })
}

fn verify_reified_cached_effect_inputs(reified: &Reified) -> Result<bool, RuntimeError> {
    let Reified::Value(value) = reified else {
        return Ok(true);
    };

    verify_cached_effect_inputs(value)
        .map_err(|message| RuntimeError::CacheInvalidation { message })
}

impl Stored {
    fn kind(&self) -> RuntimeStoredKind {
        match self {
            Self::Term(_) => RuntimeStoredKind::Term,
            Self::Value(_) => RuntimeStoredKind::Value,
        }
    }
}

fn touch_access_order(order: &mut Vec<Digest>, digest: Digest) {
    if let Some(index) = order.iter().position(|entry| *entry == digest) {
        order.remove(index);
    }
    order.push(digest);
}

fn handle_record_get(yielded: Yielded) -> Result<BuiltinOutcome, RuntimeError> {
    let mut args = yielded.args.into_iter();
    let record = args
        .next()
        .ok_or_else(|| record_get_error("expected a record and field name"))?;
    let field = args
        .next()
        .ok_or_else(|| record_get_error("expected a record and field name"))?;
    if args.next().is_some() {
        return Err(record_get_error("expected exactly two arguments"));
    }

    let RuntimeValue::Data(Value::Record(record)) = record else {
        return Err(record_get_error("first argument must be a record"));
    };
    let RuntimeValue::Data(Value::Bytes(field)) = field else {
        return Err(record_get_error(
            "second argument must be a bytes field name",
        ));
    };
    let name =
        String::from_utf8(field).map_err(|_| record_get_error("field name must be utf-8"))?;
    let value = record
        .get(&Symbol::from(name.as_str()))
        .cloned()
        .ok_or_else(|| record_get_error(format!("missing field `{name}`")))?;
    let result = yielded
        .continuation
        .resume(RuntimeValue::Data(value))
        .map_err(RuntimeError::from)?;

    Ok(BuiltinOutcome {
        result,
        cacheable: true,
        uncacheable_effect: None,
        uncacheable_policy: None,
    })
}

fn record_get_error(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Builtin {
        op: Symbol::from(RECORD_GET_OP),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::host::{Host, HostEffectPolicy};

    fn counting_host(counter: Arc<Mutex<usize>>) -> Host {
        let mut host = Host::new();
        host.register_with_policy(
            "count.tick",
            HostEffectPolicy::stable(),
            move |_args, continuation| {
                let mut value = counter.lock().expect("counter should not be poisoned");
                *value += 1;
                continuation
                    .resume(RuntimeValue::Data(Value::Integer(1)))
                    .map_err(Into::into)
            },
        );
        host
    }

    #[test]
    fn evaluates_stored_terms_by_ref() {
        let mut runtime = Runtime::new();
        let reference = runtime
            .intern_term(Term::Apply {
                callee: Box::new(Term::lambda(1, Term::var(0))),
                args: vec![Term::Value(Value::Integer(7))],
            })
            .expect("term should store");

        let result = runtime.eval_ref(&reference).expect("ref should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Integer(value))) => assert_eq!(value, 7),
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn evaluates_stored_values_by_ref() {
        let mut runtime = Runtime::new();
        let reference = runtime
            .intern_value(Value::Symbol(Symbol::from("ok")))
            .expect("value should store");

        let result = runtime.eval_ref(&reference).expect("ref should evaluate");

        match result {
            EvalResult::Done(RuntimeValue::Data(Value::Symbol(symbol))) => {
                assert_eq!(symbol, Symbol::from("ok"))
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[test]
    fn memoizes_closed_pure_results() {
        let mut runtime = Runtime::new();
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Value(Value::Integer(9))],
        };

        let first = runtime
            .eval(term.clone())
            .expect("first eval should succeed");
        let second = runtime.eval(term).expect("second eval should succeed");

        assert_eq!(runtime.memo_len(), 1);
        assert!(matches!(
            first,
            EvalResult::Done(RuntimeValue::Data(Value::Integer(9)))
        ));
        assert!(matches!(
            second,
            EvalResult::Done(RuntimeValue::Data(Value::Integer(9)))
        ));
    }

    #[test]
    fn memoizes_closed_closure_results() {
        let mut runtime = Runtime::new();
        let term = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::lambda(1, Term::var(1)))),
            args: vec![Term::Value(Value::Integer(4))],
        };

        let first = runtime
            .eval(term.clone())
            .expect("first eval should succeed");
        let second = runtime.eval(term).expect("second eval should succeed");

        assert_eq!(runtime.memo_len(), 1);
        assert!(matches!(first, EvalResult::Done(RuntimeValue::Closure(_))));
        assert!(matches!(second, EvalResult::Done(RuntimeValue::Closure(_))));
    }

    #[test]
    fn yielded_effects_are_not_memoized() {
        let mut runtime = Runtime::new();
        let term = Term::Perform {
            op: Symbol::from("fs.read"),
            args: vec![Term::Value(Value::Symbol(Symbol::from("x")))],
        };

        let result = runtime.eval(term).expect("evaluation should succeed");

        assert!(matches!(result, EvalResult::Yielded(_)));
        assert_eq!(runtime.memo_len(), 0);
    }

    #[test]
    fn thunk_force_computes_once_and_then_uses_cache() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 1);
        assert_eq!(runtime.thunk_cache_len(), 1);
    }

    #[test]
    fn nested_thunks_share_their_inner_cache() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let program = thunk::force(thunk::delay(Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: Vec::new(),
            })],
        }));

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 1);
        assert_eq!(runtime.thunk_cache_len(), 2);
    }

    #[test]
    fn force_all_records_tasks_and_preserves_result_order() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        let program = thunk::force_all([
            thunk::delay(Term::Value(Value::Integer(1))),
            thunk::delay(Term::Value(Value::Integer(2))),
            thunk::delay(Term::Value(Value::Integer(3))),
        ]);

        let traced = runtime
            .run_with_trace(program, &mut host)
            .expect("force_all should run");

        assert_eq!(
            traced.value.as_data(),
            Some(&Value::List(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]))
        );
        let summary = traced.trace.summary();
        assert_eq!(summary.thunk_force_all, 1);
        assert_eq!(summary.task_starts, 3);
        assert_eq!(summary.task_ends, 3);
    }

    #[test]
    fn force_all_reuses_shared_thunk_cache_after_first_batch() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let program = thunk::force_all([
            thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: vec![Term::Value(Value::Integer(1))],
            }),
            thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: vec![Term::Value(Value::Integer(2))],
            }),
        ]);

        let first = runtime
            .run_with_trace(program.clone(), &mut host)
            .expect("first force_all should run");
        let second = runtime
            .run_with_trace(program, &mut host)
            .expect("second force_all should run");

        assert_eq!(
            first.value.as_data(),
            Some(&Value::List(vec![Value::Integer(1), Value::Integer(1)]))
        );
        assert_eq!(
            second.value.as_data(),
            Some(&Value::List(vec![Value::Integer(1), Value::Integer(1)]))
        );
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 2);
        assert!(second.trace.summary().thunk_cache_hits >= 2);
    }

    #[test]
    fn force_all_keeps_volatile_branches_out_of_thunk_cache() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = Host::new();
        host.register("count.tick", {
            let counter = counter.clone();
            move |args, continuation| {
                let mut value = counter.lock().expect("counter should not be poisoned");
                *value += 1;
                let result = args
                    .first()
                    .and_then(RuntimeValue::as_data)
                    .cloned()
                    .unwrap_or(Value::Integer(i64::from(*value)));
                continuation
                    .resume(RuntimeValue::Data(result))
                    .map_err(Into::into)
            }
        });
        let mut runtime = Runtime::new();
        let program = thunk::force_all([
            thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: vec![Term::Value(Value::Integer(1))],
            }),
            thunk::delay(Term::Perform {
                op: Symbol::from("count.tick"),
                args: vec![Term::Value(Value::Integer(2))],
            }),
        ]);

        let traced = runtime
            .run_with_trace(program, &mut host)
            .expect("force_all should run");

        assert_eq!(
            traced.value.as_data(),
            Some(&Value::List(vec![Value::Integer(1), Value::Integer(2)]))
        );
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 2);
        assert_eq!(runtime.thunk_cache_len(), 0);
        assert_eq!(traced.trace.summary().thunk_cache_bypasses, 4);
    }

    #[test]
    fn force_all_propagates_branch_cancellation() {
        let cancellation = CancellationToken::new();
        let handler_token = cancellation.clone();
        let mut host = Host::new();
        host.register("cancel.now", move |_args, continuation| {
            handler_token.cancel();
            continuation
                .resume(RuntimeValue::Data(Value::Symbol(Symbol::from("cancelled"))))
                .map_err(Into::into)
        });
        let mut runtime = Runtime::new();
        let program = thunk::force_all([
            thunk::delay(Term::Perform {
                op: Symbol::from("cancel.now"),
                args: Vec::new(),
            }),
            thunk::delay(Term::Value(Value::Integer(1))),
        ]);

        let error = runtime
            .run_with_cancellation(program, &mut host, &cancellation)
            .expect_err("cancelled force_all should fail");

        assert!(matches!(error, RuntimeError::Cancelled));
    }

    #[test]
    fn force_all_rejects_non_data_branch_results() {
        let mut runtime = Runtime::new();
        let mut host = Host::new();
        let program = thunk::force_all([
            thunk::delay(Term::Value(Value::Integer(1))),
            thunk::delay(Term::lambda(1, Term::var(0))),
        ]);

        let error = runtime
            .run(program, &mut host)
            .expect_err("force_all should reject closure results");

        assert!(matches!(
            error,
            RuntimeError::Eval(EvalError::NonDataLiteralValue(_))
        ));
    }

    #[test]
    fn volatile_host_effects_do_not_enter_thunk_cache() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = Host::new();
        host.register("count.tick", {
            let counter = counter.clone();
            move |_args, continuation| {
                let mut value = counter.lock().expect("counter should not be poisoned");
                *value += 1;
                continuation
                    .resume(RuntimeValue::Data(Value::Integer(i64::from(*value))))
                    .map_err(Into::into)
            }
        });

        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let result = runtime.run(program, &mut host).expect("program should run");

        match result {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 2),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 2);
        assert_eq!(runtime.thunk_cache_len(), 0);
    }

    #[test]
    fn traced_runs_record_thunk_and_host_activity() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = counting_host(counter.clone());
        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let traced = runtime
            .run_with_trace(program, &mut host)
            .expect("program should run");

        match traced.value {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 1);
        assert!(
            traced
                .trace
                .events()
                .iter()
                .any(|event| matches!(event, RuntimeTraceEvent::ThunkForce { .. }))
        );
        assert!(
            traced
                .trace
                .events()
                .iter()
                .any(|event| matches!(event, RuntimeTraceEvent::ThunkCacheHit { .. }))
        );
        assert!(traced.trace.events().iter().any(|event| matches!(
            event,
            RuntimeTraceEvent::HostHandle { op, policy }
                if op.as_str() == "count.tick" && *policy == HostEffectPolicy::stable()
        )));
    }

    #[test]
    fn traced_runs_record_volatile_thunk_cache_bypass() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = Host::new();
        host.register("count.tick", {
            let counter = counter.clone();
            move |_args, continuation| {
                let mut value = counter.lock().expect("counter should not be poisoned");
                *value += 1;
                continuation
                    .resume(RuntimeValue::Data(Value::Integer(1)))
                    .map_err(Into::into)
            }
        });
        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let traced = runtime
            .run_with_trace(program, &mut host)
            .expect("program should run");

        match traced.value {
            RuntimeValue::Data(Value::Integer(value)) => assert_eq!(value, 1),
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(*counter.lock().expect("counter should not be poisoned"), 2);
        assert!(traced.trace.events().iter().any(|event| matches!(
            event,
            RuntimeTraceEvent::ThunkCacheBypass {
                op: Some(op),
                policy: Some(policy),
                ..
            } if op.as_str() == "count.tick" && *policy == HostEffectPolicy::volatile()
        )));
    }

    #[test]
    fn trace_summary_counts_boundary_activity() {
        let counter = Arc::new(Mutex::new(0));
        let mut host = counting_host(counter);
        let mut runtime = Runtime::new();
        let thunk = thunk::delay(Term::Perform {
            op: Symbol::from("count.tick"),
            args: Vec::new(),
        });
        let program = Term::Apply {
            callee: Box::new(Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::lambda(1, thunk::force(Term::var(1)))),
                    args: vec![thunk::force(Term::var(0))],
                },
            )),
            args: vec![thunk],
        };

        let traced = runtime
            .run_with_trace(program, &mut host)
            .expect("program should run");
        let summary = traced.trace.summary();

        assert_eq!(summary.total_events, traced.trace.step_count());
        assert_eq!(summary.yields, 3);
        assert_eq!(summary.builtin_handles, 2);
        assert_eq!(summary.host_handles, 1);
        assert_eq!(summary.stable_host_handles, 1);
        assert_eq!(summary.volatile_host_handles, 0);
        assert_eq!(summary.declared_host_handles, 0);
        assert_eq!(summary.hermetic_host_handles, 0);
        assert_eq!(summary.thunk_forces, 2);
        assert_eq!(summary.thunk_cache_hits, 1);
        assert_eq!(summary.thunk_cache_stores, 1);
        assert_eq!(summary.persisted, 1);
        assert_eq!(summary.persisted_values, 1);
        assert_eq!(summary.persisted_terms, 0);
        assert_eq!(summary.run_completions, 2);
    }

    #[test]
    fn memo_cache_respects_configured_entry_limit() {
        let mut runtime = Runtime::new().with_memo_limit(2);
        let mut host = Host::new();

        for value in 0..5 {
            runtime
                .run(Term::Value(Value::Integer(value)), &mut host)
                .expect("program should run");
        }

        assert_eq!(runtime.max_memo_entries(), 2);
        assert!(runtime.memo_len() <= 2);
    }

    #[test]
    fn thunk_cache_respects_configured_entry_limit() {
        let mut runtime = Runtime::new().with_thunk_cache_limit(2);
        let mut host = Host::new();

        for value in 0..5 {
            runtime
                .run(
                    thunk::force(thunk::delay(Term::Value(Value::Integer(value)))),
                    &mut host,
                )
                .expect("program should run");
        }

        assert_eq!(runtime.max_thunk_cache_entries(), 2);
        assert!(runtime.thunk_cache_len() <= 2);
    }

    #[test]
    fn cancellation_stops_drive_loop_at_yield_boundary() {
        let cancellation = CancellationToken::new();
        let handler_token = cancellation.clone();
        let mut host = Host::new();
        host.register("cancel.now", move |_, continuation| {
            handler_token.cancel();
            continuation
                .resume(RuntimeValue::Data(Value::Integer(0)))
                .map_err(Into::into)
        });
        let mut runtime = Runtime::new();

        let error = runtime
            .run_with_cancellation(
                Term::Perform {
                    op: Symbol::from("cancel.now"),
                    args: Vec::new(),
                },
                &mut host,
                &cancellation,
            )
            .expect_err("cancelled run should fail");

        assert!(matches!(error, RuntimeError::Cancelled));
    }
}
