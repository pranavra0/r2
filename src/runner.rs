use std::path::PathBuf;

use crate::{
    CancellationToken, FileStore, Host, MemoryStore, ObjectStore, Ref, Runtime, RuntimeError,
    RuntimeValue, StoreError, Term, TracedRun,
};

pub struct RuntimeRunner<S = MemoryStore> {
    runtime: Runtime<S>,
    host: Host,
    cancellation: CancellationToken,
}

impl RuntimeRunner<MemoryStore> {
    pub fn memory() -> Self {
        Self::with_store(MemoryStore::new())
    }
}

impl Default for RuntimeRunner<MemoryStore> {
    fn default() -> Self {
        Self::memory()
    }
}

impl RuntimeRunner<FileStore> {
    pub fn file_store(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        FileStore::open(path).map(Self::with_store)
    }
}

impl<S> RuntimeRunner<S> {
    pub fn with_store(store: S) -> Self {
        let cancellation = CancellationToken::new();
        Self {
            runtime: Runtime::with_store(store),
            host: Host::with_cancellation(cancellation.clone()),
            cancellation,
        }
    }

    pub fn runtime(&self) -> &Runtime<S> {
        &self.runtime
    }

    pub fn runtime_mut(&mut self) -> &mut Runtime<S> {
        &mut self.runtime
    }

    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn host_mut(&mut self) -> &mut Host {
        &mut self.host
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn grant_hermetic_process_spawn(mut self) -> Self {
        self.host.install_hermetic_process_spawn();
        self
    }

    pub fn grant_service_supervision(mut self) -> Self {
        self.host.install_service_supervise();
        self
    }

    pub fn grant_filesystem_read(mut self) -> Self {
        self.host.install_fs_read();
        self
    }

    pub fn grant_filesystem_write(mut self) -> Self {
        self.host.install_fs_write();
        self
    }

    pub fn grant_clock(mut self) -> Self {
        self.host.install_clock();
        self
    }

    pub fn grant_math(mut self) -> Self {
        self.host.install_math();
        self
    }

    pub fn configure_host(mut self, configure: impl FnOnce(&mut Host)) -> Self {
        configure(&mut self.host);
        self
    }

    pub fn process_build_host(self) -> Self {
        self.grant_hermetic_process_spawn()
    }

    pub fn service_host(self) -> Self {
        self.grant_service_supervision()
    }
}

impl<S: ObjectStore> RuntimeRunner<S> {
    pub fn run(&mut self, term: Term) -> Result<RuntimeValue, RuntimeError> {
        self.runtime
            .run_with_cancellation(term, &mut self.host, &self.cancellation)
    }

    pub fn run_traced(&mut self, term: Term) -> Result<TracedRun, RuntimeError> {
        self.runtime
            .run_with_trace_and_cancellation(term, &mut self.host, &self.cancellation)
    }

    pub fn run_ref(&mut self, reference: &Ref) -> Result<RuntimeValue, RuntimeError> {
        self.runtime
            .run_ref_with_cancellation(reference, &mut self.host, &self.cancellation)
    }

    pub fn run_ref_traced(&mut self, reference: &Ref) -> Result<TracedRun, RuntimeError> {
        self.runtime.run_ref_with_trace_and_cancellation(
            reference,
            &mut self.host,
            &self.cancellation,
        )
    }
}
