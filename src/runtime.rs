use crate::{
    ActionSpec, CachedOutcome, CapSet, CellId, CellVersion, EffectKind, Failure, FailureKind,
    ForceResult, GcPlan, GcReport, GraphTrace, Hash, HostFn, Node, Outcome, Store, StoreStats,
    Tree, TreeEntry, Value,
};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

pub struct Runtime {
    store: Store,
    caps: CapSet,
}

fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Int(_) => "int",
        Value::Text(_) => "text",
        Value::Bytes(_) => "bytes",
        Value::Blob(_) => "blob",
        Value::Tree(_) => "tree",
        Value::Tuple(_) => "tuple",
        Value::Artifact(_) => "artifact",
        Value::ActionResult { .. } => "action-result",
    }
}

impl Runtime {
    pub fn new(store_path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Self::with_caps(store_path, CapSet::new())
    }

    pub fn with_caps(store_path: impl AsRef<Path>, caps: CapSet) -> anyhow::Result<Self> {
        Ok(Self {
            store: Store::open(store_path)?,
            caps,
        })
    }

    pub fn register(&mut self, name: impl Into<String>, func: HostFn) {
        self.caps.insert(name, func);
    }

    pub fn has_capability(&self, name: &str) -> bool {
        self.caps.contains(name)
    }

    pub fn capability_effect(&self, name: &str) -> Option<EffectKind> {
        self.caps.effect(name)
    }

    pub fn capability(&self, name: &str) -> Option<crate::Capability> {
        self.caps.capability(name)
    }

    pub fn capabilities(&self) -> Vec<crate::Capability> {
        self.caps.capabilities()
    }

    pub fn int(&self, value: i64) -> anyhow::Result<Hash> {
        self.value(Value::Int(value))
    }

    pub fn text(&self, value: impl Into<String>) -> anyhow::Result<Hash> {
        self.value(Value::Text(value.into()))
    }

    pub fn blob(&self, bytes: Vec<u8>) -> anyhow::Result<Hash> {
        self.value(Value::Blob(bytes))
    }

    pub fn tree(&self, entries: BTreeMap<String, TreeEntry>) -> anyhow::Result<Hash> {
        self.value(Value::Tree(Tree { entries }))
    }

    pub fn import_file(&self, path: impl AsRef<Path>) -> anyhow::Result<Hash> {
        let bytes = std::fs::read(path)?;
        self.blob(bytes)
    }

    pub fn import_tool(&self, path: impl AsRef<Path>) -> anyhow::Result<Hash> {
        self.import_file(path)
    }

    pub fn import_tree(&self, path: impl AsRef<Path>) -> anyhow::Result<Hash> {
        self.import_tree_inner(path.as_ref())
    }

    pub fn export(&self, hash: Hash, destination: impl AsRef<Path>) -> anyhow::Result<()> {
        let (value, _) = self.force_value(hash)?;
        self.materialize_value(&value, destination.as_ref())
    }

    pub fn tree_get(&self, tree: Hash, path: &str) -> anyhow::Result<Option<Hash>> {
        let (value, _) = self.force_value(tree)?;
        let Value::Tree(tree) = value else {
            anyhow::bail!("tree_get expected a tree");
        };

        let mut current = tree;
        let mut parts = path.split('/').filter(|part| !part.is_empty()).peekable();
        while let Some(part) = parts.next() {
            let Some(entry) = current.entries.get(part) else {
                return Ok(None);
            };

            match entry {
                TreeEntry::Blob(hash) => {
                    if parts.peek().is_some() {
                        return Ok(None);
                    }
                    return Ok(Some(hash.clone()));
                }
                TreeEntry::Tree(hash) => {
                    if parts.peek().is_none() {
                        return Ok(Some(hash.clone()));
                    }
                    let (value, _) = self.force_value(hash.clone())?;
                    let Value::Tree(next) = value else {
                        anyhow::bail!("tree entry {part} points to non-tree value");
                    };
                    current = next;
                }
            }
        }

        Ok(None)
    }

    pub fn tree_put(&self, tree: Hash, path: &str, entry: TreeEntry) -> anyhow::Result<Hash> {
        let parts = path
            .split('/')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        if parts.is_empty() {
            anyhow::bail!("tree_put requires a non-empty path");
        }

        let (value, _) = self.force_value(tree)?;
        let Value::Tree(mut root) = value else {
            anyhow::bail!("tree_put expected a tree");
        };

        self.tree_put_parts(&mut root, &parts, entry)?;
        self.tree(root.entries)
    }

    pub fn value(&self, value: Value) -> anyhow::Result<Hash> {
        self.store.put_value(&value)?;
        self.store.put_node(&Node::Value(value))
    }

    pub fn call(&self, function: impl Into<String>, args: Vec<Hash>) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Apply {
            function: function.into(),
            args,
        })
    }

    pub fn host_call(
        &self,
        capability: impl Into<String>,
        args: Vec<Hash>,
        effect: EffectKind,
    ) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::HostCall {
            capability: capability.into(),
            args,
            effect,
        })
    }

    pub fn action(&self, spec: ActionSpec) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Action(spec))
    }

    pub fn cell_new(&self, initial: Hash) -> anyhow::Result<CellId> {
        self.store.cell_new(initial)
    }

    pub fn cell_set(&self, id: &CellId, value: Hash) -> anyhow::Result<CellVersion> {
        self.store.cell_set(id, value)
    }

    pub fn cell_current(&self, id: &CellId) -> anyhow::Result<Option<CellVersion>> {
        self.store.cell_current(id)
    }

    pub fn read_cell(&self, id: CellId) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::ReadCell(id))
    }

    pub fn thunk(&self, target: Hash) -> anyhow::Result<Hash> {
        self.store.put_node(&Node::Thunk { target })
    }

    pub fn force(&self, node: Hash) -> anyhow::Result<ForceResult> {
        if self.node_is_cacheable(&node, &mut BTreeSet::new())?
            && let Some(cached) = self.store.get_cached_outcome(&node)?
            && self.cached_outcome_is_fresh(&cached)?
        {
            if let Some(failure) = self.validate_authority(&node)? {
                return Ok(ForceResult {
                    outcome: Outcome::Failure(failure),
                    cache_hit: false,
                });
            }

            return Ok(ForceResult {
                outcome: cached.outcome,
                cache_hit: true,
            });
        }

        let mut observed_cells = BTreeMap::new();
        let outcome = self.eval(&node, &mut GraphTrace::default(), &mut observed_cells)?;
        if self.should_cache(&node, &outcome)? {
            self.store.put_outcome(
                &node,
                &CachedOutcome {
                    outcome: outcome.clone(),
                    observed_cells,
                },
            )?;
        }
        Ok(ForceResult {
            outcome,
            cache_hit: false,
        })
    }

    pub fn get_node(&self, hash: &Hash) -> anyhow::Result<Option<Node>> {
        self.store.get_node(hash)
    }

    pub fn get_outcome(&self, hash: &Hash) -> anyhow::Result<Option<Outcome>> {
        self.store.get_outcome(hash)
    }

    pub fn force_value(&self, node: Hash) -> anyhow::Result<(Value, bool)> {
        let forced = self.force(node)?;
        match forced.outcome {
            Outcome::Success(value_hash) => {
                let value = self.get_value(&value_hash)?;
                Ok((value, forced.cache_hit))
            }
            Outcome::Failure(failure) => Err(anyhow::anyhow!("{failure:?}")),
        }
    }

    pub fn get_value(&self, hash: &Hash) -> anyhow::Result<Value> {
        self.store
            .get_value(hash)?
            .ok_or_else(|| anyhow::anyhow!("missing value {hash}"))
    }

    pub fn pin(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        self.store.pin(name, hash)
    }

    pub fn unpin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.unpin(name)
    }

    pub fn resolve_pin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.resolve_pin(name)
    }

    pub fn pins(&self) -> anyhow::Result<std::collections::BTreeMap<String, Hash>> {
        self.store.pins()
    }

    pub fn alias(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        self.store.alias(name, hash)
    }

    pub fn unalias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.unalias(name)
    }

    pub fn resolve_alias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        self.store.resolve_alias(name)
    }

    pub fn aliases(&self) -> anyhow::Result<std::collections::BTreeMap<String, Hash>> {
        self.store.aliases()
    }

    pub fn store_stats(&self) -> anyhow::Result<StoreStats> {
        self.store.stats()
    }

    pub fn gc_plan(&self) -> anyhow::Result<GcPlan> {
        self.store.gc_plan()
    }

    pub fn gc(&self) -> anyhow::Result<GcReport> {
        self.store.gc()
    }

    pub fn explain(&self, hash: &Hash) -> anyhow::Result<String> {
        let mut out = String::new();
        self.explain_hash(hash, &mut out, 0)?;
        Ok(out)
    }

    fn explain_hash(&self, hash: &Hash, out: &mut String, indent: usize) -> anyhow::Result<()> {
        let pad = " ".repeat(indent);
        if let Some(node) = self.get_node(hash)? {
            match node {
                Node::Value(value) => {
                    out.push_str(&format!("{pad}{hash} value-node {}\n", value_kind(&value)));
                }
                Node::Thunk { target } => {
                    out.push_str(&format!("{pad}{hash} thunk\n"));
                    self.explain_hash(&target, out, indent + 2)?;
                }
                Node::Apply { function, args } => {
                    out.push_str(&format!("{pad}{hash} apply {function}\n"));
                    for arg in args {
                        self.explain_hash(&arg, out, indent + 2)?;
                    }
                }
                Node::HostCall {
                    capability,
                    args,
                    effect,
                } => {
                    out.push_str(&format!("{pad}{hash} host-call {capability} {effect:?}\n"));
                    for arg in args {
                        self.explain_hash(&arg, out, indent + 2)?;
                    }
                }
                Node::Action(spec) => {
                    out.push_str(&format!("{pad}{hash} action {}\n", spec.program));
                    out.push_str(&format!("{pad}  tool {}\n", spec.tool));
                    out.push_str(&format!("{pad}  platform {}\n", spec.platform));
                    for input in spec.inputs {
                        out.push_str(&format!("{pad}  input {} {}\n", input.path, input.hash));
                    }
                    for output in spec.outputs {
                        out.push_str(&format!("{pad}  output {output}\n"));
                    }
                }
                Node::ReadCell(id) => {
                    out.push_str(&format!("{pad}{hash} read-cell {}\n", id.0));
                    if let Some(version) = self.store.cell_current(&id)? {
                        out.push_str(&format!(
                            "{pad}  current version {} {}\n",
                            version.index, version.value
                        ));
                        self.explain_hash(&version.value, out, indent + 2)?;
                    }
                }
            }
        } else if let Ok(value) = self.get_value(hash) {
            out.push_str(&format!("{pad}{hash} value {}\n", value_kind(&value)));
            if let Value::ActionResult {
                outputs,
                stdout,
                stderr,
            } = value
            {
                out.push_str(&format!("{pad}  outputs {outputs}\n"));
                out.push_str(&format!("{pad}  stdout {stdout}\n"));
                out.push_str(&format!("{pad}  stderr {stderr}\n"));
            }
        } else {
            out.push_str(&format!("{pad}{hash} missing\n"));
        }

        if let Some(cached) = self.store.get_cached_outcome(hash)? {
            match cached.outcome {
                Outcome::Success(value) => {
                    out.push_str(&format!("{pad}=> success {value}\n"));
                }
                Outcome::Failure(failure) => {
                    out.push_str(&format!("{pad}=> failure {}\n", failure.kind));
                    out.push_str(&format!("{pad}trace:\n"));
                    for step in failure.trace.hashes() {
                        out.push_str(&format!("{pad}  -> {step}\n"));
                    }
                    if let FailureKind::ActionFailed { stdout, stderr, .. } = failure.kind {
                        if !stdout.is_empty() {
                            out.push_str(&format!("{pad}stdout:\n{stdout}\n"));
                        }
                        if !stderr.is_empty() {
                            out.push_str(&format!("{pad}stderr:\n{stderr}\n"));
                        }
                    }
                }
            }
            if !cached.observed_cells.is_empty() {
                out.push_str(&format!("{pad}observed cells:\n"));
                for (cell, version) in cached.observed_cells {
                    out.push_str(&format!("{pad}  {} @ {}\n", cell.0, version));
                }
            }
        }

        Ok(())
    }

    fn import_tree_inner(&self, path: &Path) -> anyhow::Result<Hash> {
        let mut entries = BTreeMap::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name = file_name
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("path contains non-utf8 entry: {:?}", file_name))?;

            let file_type = entry.file_type()?;
            let entry_path = entry.path();
            if file_type.is_dir() {
                let tree = self.import_tree_inner(&entry_path)?;
                entries.insert(name.to_owned(), TreeEntry::Tree(tree));
            } else if file_type.is_file() {
                let blob = self.import_file(&entry_path)?;
                entries.insert(name.to_owned(), TreeEntry::Blob(blob));
            }
        }

        self.tree(entries)
    }

    fn tree_put_parts(
        &self,
        tree: &mut Tree,
        parts: &[&str],
        entry: TreeEntry,
    ) -> anyhow::Result<()> {
        if parts.len() == 1 {
            tree.entries.insert(parts[0].to_owned(), entry);
            return Ok(());
        }

        let head = parts[0];
        let child = match tree.entries.get(head) {
            Some(TreeEntry::Tree(hash)) => {
                let (value, _) = self.force_value(hash.clone())?;
                let Value::Tree(tree) = value else {
                    anyhow::bail!("tree entry {head} points to non-tree value");
                };
                tree
            }
            Some(TreeEntry::Blob(_)) => {
                anyhow::bail!("cannot put below blob at {head}");
            }
            None => Tree {
                entries: BTreeMap::new(),
            },
        };

        let mut child = child;
        self.tree_put_parts(&mut child, &parts[1..], entry)?;
        let child_hash = self.tree(child.entries)?;
        tree.entries
            .insert(head.to_owned(), TreeEntry::Tree(child_hash));
        Ok(())
    }

    fn eval(
        &self,
        node_hash: &Hash,
        trace: &mut GraphTrace,
        observed_cells: &mut BTreeMap<CellId, u64>,
    ) -> anyhow::Result<Outcome> {
        if trace.contains(node_hash) {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::Cycle(node_hash.clone()),
                trace.clone(),
            )));
        }

        trace.push(node_hash.clone());

        let Some(node) = self.store.get_node(node_hash)? else {
            let failure = Failure::new(
                node_hash.clone(),
                FailureKind::MissingObject(node_hash.clone()),
                trace.clone(),
            );
            trace.pop();
            return Ok(Outcome::Failure(failure));
        };

        let outcome = match node {
            Node::Value(value) => {
                let value_hash = self.store.put_value(&value)?;
                Outcome::Success(value_hash)
            }
            Node::Thunk { target } => self.force_dependency(&target, trace, observed_cells)?,
            Node::Apply { function, args } => self.eval_apply(
                node_hash,
                &function,
                &args,
                EffectKind::Pure,
                trace,
                observed_cells,
            )?,
            Node::HostCall {
                capability,
                args,
                effect,
            } => self.eval_apply(node_hash, &capability, &args, effect, trace, observed_cells)?,
            Node::Action(spec) => self.eval_action(node_hash, &spec, trace, observed_cells)?,
            Node::ReadCell(id) => {
                let Some(version) = self.store.cell_current(&id)? else {
                    return Ok(Outcome::Failure(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCell(id.0),
                        trace.clone(),
                    )));
                };
                observed_cells.insert(id, version.index);
                match self.force_dependency(&version.value, trace, observed_cells)? {
                    Outcome::Success(value) => Outcome::Success(value),
                    Outcome::Failure(failure) => Outcome::Failure(failure),
                }
            }
        };

        trace.pop();
        Ok(outcome)
    }

    fn eval_apply(
        &self,
        node_hash: &Hash,
        function: &str,
        args: &[Hash],
        requested_effect: EffectKind,
        trace: &mut GraphTrace,
        observed_cells: &mut BTreeMap<CellId, u64>,
    ) -> anyhow::Result<Outcome> {
        let Some(cap) = self.caps.get(function) else {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::UnknownCapability(function.to_owned()),
                trace.clone(),
            )));
        };

        let actual_effect = cap.effect();
        if actual_effect != requested_effect {
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::EffectMismatch {
                    capability: function.to_owned(),
                    requested: requested_effect,
                    actual: actual_effect,
                },
                trace.clone(),
            )));
        }

        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            match self.force_dependency(arg, trace, observed_cells)? {
                Outcome::Success(value_hash) => {
                    let Some(value) = self.store.get_value(&value_hash)? else {
                        return Ok(Outcome::Failure(Failure::new(
                            node_hash.clone(),
                            FailureKind::MissingObject(value_hash),
                            trace.clone(),
                        )));
                    };
                    values.push(value);
                }
                Outcome::Failure(failure) => return Ok(Outcome::Failure(failure)),
            }
        }

        match cap.call(&values) {
            Ok(value) => {
                let value_hash = self.store.put_value(&value)?;
                Ok(Outcome::Success(value_hash))
            }
            Err(kind) => Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                kind,
                trace.clone(),
            ))),
        }
    }

    fn eval_action(
        &self,
        node_hash: &Hash,
        spec: &ActionSpec,
        trace: &mut GraphTrace,
        observed_cells: &mut BTreeMap<CellId, u64>,
    ) -> anyhow::Result<Outcome> {
        let workspace = self.new_workspace()?;
        for input in &spec.inputs {
            let destination = workspace.join(&input.path);
            self.materialize_hash(&input.hash, &destination, trace, observed_cells)?;
        }

        let output = Command::new(&spec.program)
            .args(&spec.args)
            .env_clear()
            .envs(&spec.env)
            .current_dir(&workspace)
            .output();

        let output = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = std::fs::remove_dir_all(&workspace);
                return Ok(Outcome::Failure(Failure::new(
                    node_hash.clone(),
                    FailureKind::Host(format!("failed to run action {}: {error}", spec.program)),
                    trace.clone(),
                )));
            }
        };

        if !output.status.success() {
            let _ = std::fs::remove_dir_all(&workspace);
            return Ok(Outcome::Failure(Failure::new(
                node_hash.clone(),
                FailureKind::ActionFailed {
                    program: spec.program.clone(),
                    status: output
                        .status
                        .code()
                        .map(|code| code.to_string())
                        .unwrap_or_else(|| "signal".to_owned()),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                },
                trace.clone(),
            )));
        }

        let mut entries = BTreeMap::new();
        for output_path in &spec.outputs {
            let path = workspace.join(output_path);
            if path.is_file() {
                let hash = self.import_file(&path)?;
                entries.insert(output_path.clone(), TreeEntry::Blob(hash));
            } else if path.is_dir() {
                let hash = self.import_tree(&path)?;
                entries.insert(output_path.clone(), TreeEntry::Tree(hash));
            } else {
                let _ = std::fs::remove_dir_all(&workspace);
                return Ok(Outcome::Failure(Failure::new(
                    node_hash.clone(),
                    FailureKind::MissingActionOutput(output_path.clone()),
                    trace.clone(),
                )));
            }
        }

        let _ = std::fs::remove_dir_all(&workspace);
        let output_tree = self.tree(entries)?;
        let stdout = self.blob(output.stdout)?;
        let stderr = self.blob(output.stderr)?;
        let value_hash = self.store.put_value(&Value::ActionResult {
            outputs: output_tree,
            stdout,
            stderr,
        })?;
        Ok(Outcome::Success(value_hash))
    }

    fn materialize_hash(
        &self,
        hash: &Hash,
        destination: &Path,
        trace: &mut GraphTrace,
        observed_cells: &mut BTreeMap<CellId, u64>,
    ) -> anyhow::Result<()> {
        let outcome = self.force_dependency(hash, trace, observed_cells)?;
        let Outcome::Success(value_hash) = outcome else {
            anyhow::bail!("cannot materialize failed input {hash}");
        };
        let Some(value) = self.store.get_value(&value_hash)? else {
            anyhow::bail!("input {hash} resolved to missing value {value_hash}");
        };
        self.materialize_value(&value, destination)
    }

    fn materialize_value(&self, value: &Value, destination: &Path) -> anyhow::Result<()> {
        match value {
            Value::Blob(bytes) | Value::Bytes(bytes) => {
                if let Some(parent) = destination.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(destination, bytes)?;
            }
            Value::Tree(tree) => {
                std::fs::create_dir_all(destination)?;
                for (name, entry) in &tree.entries {
                    let child = destination.join(name);
                    match entry {
                        TreeEntry::Blob(hash) | TreeEntry::Tree(hash) => {
                            let (value, _) = self.force_value(hash.clone())?;
                            self.materialize_value(&value, &child)?;
                        }
                    }
                }
            }
            other => anyhow::bail!("cannot materialize value as file/tree: {other:?}"),
        }
        Ok(())
    }

    fn new_workspace(&self) -> anyhow::Result<std::path::PathBuf> {
        let path = std::env::temp_dir().join(format!(
            "r2-action-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(path)
    }

    fn force_dependency(
        &self,
        node: &Hash,
        trace: &mut GraphTrace,
        observed_cells: &mut BTreeMap<CellId, u64>,
    ) -> anyhow::Result<Outcome> {
        if self.node_is_cacheable(node, &mut BTreeSet::new())?
            && let Some(cached) = self.store.get_cached_outcome(node)?
            && self.cached_outcome_is_fresh(&cached)?
        {
            if let Some(failure) = self.validate_authority_with_trace(node, trace.clone())? {
                return Ok(Outcome::Failure(failure));
            }
            observed_cells.extend(cached.observed_cells.clone());
            return Ok(self.with_current_trace(cached.outcome, trace));
        }

        let mut dependency_observed_cells = BTreeMap::new();
        let outcome = self.eval(node, trace, &mut dependency_observed_cells)?;
        observed_cells.extend(dependency_observed_cells.clone());
        if self.should_cache(node, &outcome)? {
            self.store.put_outcome(
                node,
                &CachedOutcome {
                    outcome: outcome.clone(),
                    observed_cells: dependency_observed_cells,
                },
            )?;
        }
        Ok(outcome)
    }

    fn cached_outcome_is_fresh(&self, cached: &CachedOutcome) -> anyhow::Result<bool> {
        for (cell, observed) in &cached.observed_cells {
            let Some(current) = self.store.cell_current(cell)? else {
                return Ok(false);
            };
            if current.index != *observed {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn with_current_trace(&self, outcome: Outcome, trace: &GraphTrace) -> Outcome {
        let Outcome::Failure(mut failure) = outcome else {
            return outcome;
        };

        if trace.hashes().is_empty() {
            return Outcome::Failure(failure);
        }

        let mut dependency_path = trace.hashes().to_vec();
        for hash in failure.trace.hashes() {
            if dependency_path.last() != Some(hash) {
                dependency_path.push(hash.clone());
            }
        }
        failure.trace = GraphTrace::new(dependency_path);
        Outcome::Failure(failure)
    }

    fn validate_authority(&self, node: &Hash) -> anyhow::Result<Option<Failure>> {
        self.validate_authority_with_trace(node, GraphTrace::default())
    }

    fn validate_authority_with_trace(
        &self,
        node: &Hash,
        mut trace: GraphTrace,
    ) -> anyhow::Result<Option<Failure>> {
        self.validate_authority_inner(node, &mut trace, &mut BTreeSet::new())
    }

    fn validate_authority_inner(
        &self,
        node_hash: &Hash,
        trace: &mut GraphTrace,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Failure>> {
        if !visited.insert(node_hash.clone()) {
            return Ok(None);
        }

        trace.push(node_hash.clone());
        let Some(node) = self.store.get_node(node_hash)? else {
            trace.pop();
            return Ok(None);
        };

        let failure = match node {
            Node::Value(_) => None,
            Node::Thunk { target } => self.validate_authority_inner(&target, trace, visited)?,
            Node::Apply { function, args } => {
                if let Some(cap) = self.caps.get(&function) {
                    let actual_effect = cap.effect();
                    if actual_effect != EffectKind::Pure {
                        Some(Failure::new(
                            node_hash.clone(),
                            FailureKind::EffectMismatch {
                                capability: function,
                                requested: EffectKind::Pure,
                                actual: actual_effect,
                            },
                            trace.clone(),
                        ))
                    } else {
                        self.validate_args_authority(&args, trace, visited)?
                    }
                } else {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(function),
                        trace.clone(),
                    ))
                }
            }
            Node::HostCall {
                capability,
                args,
                effect,
            } => {
                if let Some(cap) = self.caps.get(&capability) {
                    let actual_effect = cap.effect();
                    if actual_effect != effect {
                        Some(Failure::new(
                            node_hash.clone(),
                            FailureKind::EffectMismatch {
                                capability,
                                requested: effect,
                                actual: actual_effect,
                            },
                            trace.clone(),
                        ))
                    } else {
                        self.validate_args_authority(&args, trace, visited)?
                    }
                } else {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCapability(capability),
                        trace.clone(),
                    ))
                }
            }
            Node::Action(spec) => self.validate_action_authority(&spec, trace, visited)?,
            Node::ReadCell(id) => {
                if let Some(version) = self.store.cell_current(&id)? {
                    self.validate_authority_inner(&version.value, trace, visited)?
                } else {
                    Some(Failure::new(
                        node_hash.clone(),
                        FailureKind::UnknownCell(id.0),
                        trace.clone(),
                    ))
                }
            }
        };

        trace.pop();
        Ok(failure)
    }

    fn validate_args_authority(
        &self,
        args: &[Hash],
        trace: &mut GraphTrace,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Failure>> {
        for arg in args {
            if let Some(failure) = self.validate_authority_inner(arg, trace, visited)? {
                return Ok(Some(failure));
            }
        }

        Ok(None)
    }

    fn validate_action_authority(
        &self,
        spec: &ActionSpec,
        trace: &mut GraphTrace,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<Option<Failure>> {
        if let Some(failure) = self.validate_authority_inner(&spec.tool, trace, visited)? {
            return Ok(Some(failure));
        }
        for input in &spec.inputs {
            if let Some(failure) = self.validate_authority_inner(&input.hash, trace, visited)? {
                return Ok(Some(failure));
            }
        }
        Ok(None)
    }

    fn should_cache(&self, node: &Hash, outcome: &Outcome) -> anyhow::Result<bool> {
        Ok(outcome.is_cacheable() && self.node_is_cacheable(node, &mut BTreeSet::new())?)
    }

    fn node_is_cacheable(
        &self,
        node_hash: &Hash,
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<bool> {
        if !visited.insert(node_hash.clone()) {
            return Ok(true);
        }

        let Some(node) = self.store.get_node(node_hash)? else {
            return Ok(false);
        };

        match node {
            Node::Value(_) => Ok(true),
            Node::Thunk { target } => self.node_is_cacheable(&target, visited),
            Node::Apply { args, .. } => self.args_are_cacheable(&args, visited),
            Node::HostCall { effect, args, .. } => {
                if effect == EffectKind::Live {
                    Ok(false)
                } else {
                    self.args_are_cacheable(&args, visited)
                }
            }
            Node::Action(spec) => {
                if !self.node_is_cacheable(&spec.tool, visited)? {
                    return Ok(false);
                }
                for input in &spec.inputs {
                    if !self.node_is_cacheable(&input.hash, visited)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Node::ReadCell(_) => Ok(true),
        }
    }

    fn args_are_cacheable(
        &self,
        args: &[Hash],
        visited: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<bool> {
        for arg in args {
            if !self.node_is_cacheable(arg, visited)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn add_ints(args: &[Value]) -> Result<Value, FailureKind> {
        match args {
            [Value::Int(a), Value::Int(b)] => Ok(Value::Int(a + b)),
            _ => Err(FailureKind::TypeError("+ expects two ints".to_owned())),
        }
    }

    fn temp_store() -> anyhow::Result<PathBuf> {
        Ok(std::env::temp_dir().join(format!(
            "r2-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        )))
    }

    #[test]
    fn forces_pure_node_and_reuses_cached_outcome() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr)?;

        let first = rt.force(sum.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(value_hash) = first.outcome else {
            panic!("sum should succeed");
        };
        assert_eq!(rt.get_value(&value_hash)?, Value::Int(42));

        let second = rt.force(sum)?;
        assert!(second.cache_hit);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn exposes_capability_metadata() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));
        rt.register("clock", HostFn::live(|_| Ok(Value::Int(1))));

        assert!(rt.has_capability("+"));
        assert!(!rt.has_capability("network"));
        assert_eq!(rt.capability_effect("+"), Some(EffectKind::Pure));
        assert_eq!(rt.capability_effect("clock"), Some(EffectKind::Live));
        assert_eq!(rt.capability_effect("network"), None);
        assert_eq!(
            rt.capability("+"),
            Some(crate::Capability {
                name: "+".to_owned(),
                effect: EffectKind::Pure,
            })
        );

        let caps = rt.capabilities();
        assert_eq!(caps.len(), 2);
        assert_eq!(caps[0].name, "+");
        assert_eq!(caps[1].name, "clock");

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn fails_when_capability_is_not_registered() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let a = rt.int(1)?;
        let b = rt.int(2)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr.clone())?;

        let forced = rt.force(sum.clone())?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("unknown capability should fail");
        };
        assert_eq!(failure.kind, FailureKind::UnknownCapability("+".to_owned()));
        assert_eq!(failure.trace.hashes(), &[sum, sum_expr]);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn reuses_cached_outcome_across_runtime_instances() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;

            let first = rt.force(sum.clone())?;
            assert!(!first.cache_hit);
            sum
        };

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let second = rt.force(sum)?;
        assert!(second.cache_hit);
        let Outcome::Success(value_hash) = second.outcome else {
            panic!("cached sum should succeed");
        };
        assert_eq!(rt.get_value(&value_hash)?, Value::Int(42));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn can_inspect_graph_and_cached_outcome() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr.clone())?;

        assert_eq!(
            rt.get_node(&sum)?,
            Some(Node::Thunk {
                target: sum_expr.clone()
            })
        );
        assert_eq!(rt.get_outcome(&sum)?, None);

        let forced = rt.force(sum.clone())?;
        assert!(!forced.cache_hit);
        assert_eq!(rt.get_outcome(&sum)?, Some(forced.outcome));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn pins_persist_across_runtime_instances() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;
            rt.pin("demo.sum", sum.clone())?;
            sum
        };

        let rt = Runtime::new(&temp)?;
        assert_eq!(rt.resolve_pin("demo.sum")?, Some(sum.clone()));
        assert_eq!(rt.resolve_pin("missing")?, None);
        assert_eq!(rt.pins()?.get("demo.sum"), Some(&sum));

        let removed = rt.unpin("demo.sum")?;
        assert_eq!(removed, Some(sum));
        assert_eq!(rt.resolve_pin("demo.sum")?, None);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn aliases_persist_and_are_distinct_from_pins() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let (first_sum, second_sum) = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let first_expr = rt.call("+", vec![a.clone(), b.clone()])?;
            let first_sum = rt.thunk(first_expr)?;

            let c = rt.int(1)?;
            let second_expr = rt.call("+", vec![first_sum.clone(), c])?;
            let second_sum = rt.thunk(second_expr)?;

            rt.alias("demo.sum", first_sum.clone())?;
            assert_eq!(rt.resolve_alias("demo.sum")?, Some(first_sum.clone()));
            assert_eq!(rt.resolve_pin("demo.sum")?, None);

            rt.alias("demo.sum", second_sum.clone())?;
            (first_sum, second_sum)
        };

        let rt = Runtime::new(&temp)?;
        assert_eq!(rt.resolve_alias("demo.sum")?, Some(second_sum.clone()));
        assert_eq!(rt.aliases()?.get("demo.sum"), Some(&second_sum));
        assert_eq!(rt.resolve_pin("demo.sum")?, None);

        let removed = rt.unalias("demo.sum")?;
        assert_eq!(removed.as_ref(), Some(&second_sum));
        assert_eq!(rt.resolve_alias("demo.sum")?, None);
        assert_eq!(rt.resolve_pin("demo.sum")?, None);
        assert_ne!(first_sum, second_sum);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn reports_store_stats() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr)?;
        rt.force(sum.clone())?;
        rt.pin("demo.sum", sum)?;
        let alias_target = rt.int(7)?;
        rt.alias("demo.seven", alias_target)?;

        let stats = rt.store_stats()?;
        assert!(stats.object_count >= 5);
        assert!(stats.outcome_count >= 1);
        assert_eq!(stats.root_count, 1);
        assert_eq!(stats.alias_count, 1);
        assert!(stats.total_bytes > 0);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn rejects_object_kind_mismatch() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let value_node = rt.int(42)?;
        let Outcome::Success(value_hash) = rt.force(value_node)?.outcome else {
            panic!("value node should force");
        };

        let err = rt.get_node(&value_hash).unwrap_err();
        assert!(err.to_string().contains("expected Node"));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn cached_success_still_requires_current_capability() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let sum = {
            let mut rt = Runtime::new(&temp)?;
            rt.register("+", HostFn::pure(add_ints));

            let a = rt.int(20)?;
            let b = rt.int(22)?;
            let sum_expr = rt.call("+", vec![a, b])?;
            let sum = rt.thunk(sum_expr)?;

            let first = rt.force(sum.clone())?;
            assert!(!first.cache_hit);
            let Outcome::Success(_) = first.outcome else {
                panic!("sum should succeed");
            };
            sum
        };

        let rt = Runtime::new(&temp)?;
        let forced = rt.force(sum)?;
        assert!(!forced.cache_hit);
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("missing capability should block cached success");
        };
        assert_eq!(failure.kind, FailureKind::UnknownCapability("+".to_owned()));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn pure_apply_rejects_live_capability() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("clock", HostFn::live(|_| Ok(Value::Int(1))));

        let clock = rt.call("clock", vec![])?;
        let forced = rt.force(clock)?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("pure apply to live cap should fail");
        };
        assert_eq!(
            failure.kind,
            FailureKind::EffectMismatch {
                capability: "clock".to_owned(),
                requested: EffectKind::Pure,
                actual: EffectKind::Live,
            }
        );
        assert_eq!(rt.get_outcome(&failure.node)?, None);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn live_host_call_is_not_cached() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let counter = Arc::new(AtomicI64::new(0));
        let counter_for_cap = Arc::clone(&counter);
        let mut rt = Runtime::new(&temp)?;
        rt.register(
            "next",
            HostFn::live(move |_| {
                let next = counter_for_cap.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Value::Int(next))
            }),
        );

        let next = rt.host_call("next", vec![], EffectKind::Live)?;
        let first = rt.force(next.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(first_hash) = first.outcome else {
            panic!("live call should succeed");
        };
        assert_eq!(rt.get_value(&first_hash)?, Value::Int(1));
        assert_eq!(rt.get_outcome(&next)?, None);

        let second = rt.force(next)?;
        assert!(!second.cache_hit);
        let Outcome::Success(second_hash) = second.outcome else {
            panic!("live call should succeed again");
        };
        assert_eq!(rt.get_value(&second_hash)?, Value::Int(2));
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn host_call_rejects_wrong_declared_effect() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(1)?;
        let b = rt.int(2)?;
        let sum = rt.host_call("+", vec![a, b], EffectKind::Live)?;
        let forced = rt.force(sum)?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("wrong host call effect should fail");
        };
        assert_eq!(
            failure.kind,
            FailureKind::EffectMismatch {
                capability: "+".to_owned(),
                requested: EffectKind::Live,
                actual: EffectKind::Pure,
            }
        );

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_plan_traces_reachable_graph_from_pins() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a.clone(), b.clone()])?;
        let sum = rt.thunk(sum_expr.clone())?;
        let forced = rt.force(sum.clone())?;
        let Outcome::Success(value_hash) = forced.outcome else {
            panic!("sum should succeed");
        };
        rt.pin("demo.sum", sum.clone())?;

        let plan = rt.gc_plan()?;
        assert_eq!(plan.roots.get("demo.sum"), Some(&sum));
        assert!(plan.reachable_objects.contains(&sum));
        assert!(plan.reachable_objects.contains(&sum_expr));
        assert!(plan.reachable_objects.contains(&a));
        assert!(plan.reachable_objects.contains(&b));
        assert!(plan.reachable_objects.contains(&value_hash));
        assert!(plan.reachable_outcomes.contains(&sum));
        assert!(!plan.unreachable_objects.contains(&sum));
        assert!(!plan.unreachable_outcomes.contains(&sum));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_plan_does_not_treat_aliases_as_roots() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let value = rt.int(7)?;
        rt.alias("demo.seven", value.clone())?;

        let plan = rt.gc_plan()?;
        assert_eq!(plan.roots.get("demo.seven"), None);
        assert!(!plan.reachable_objects.contains(&value));
        assert!(plan.unreachable_objects.contains(&value));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_deletes_unreachable_objects_and_preserves_pins_and_aliases() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let a = rt.int(20)?;
        let b = rt.int(22)?;
        let sum_expr = rt.call("+", vec![a, b])?;
        let sum = rt.thunk(sum_expr)?;
        rt.force(sum.clone())?;
        rt.pin("demo.sum", sum.clone())?;

        let alias_only = rt.int(7)?;
        rt.alias("demo.seven", alias_only.clone())?;
        assert!(rt.get_node(&alias_only)?.is_some());

        let report = rt.gc()?;
        assert!(report.deleted_objects > 0);
        assert!(report.deleted_bytes > 0);
        assert!(report.plan.unreachable_objects.contains(&alias_only));

        assert!(rt.get_node(&sum)?.is_some());
        assert_eq!(rt.resolve_pin("demo.sum")?, Some(sum));
        assert_eq!(rt.resolve_alias("demo.seven")?, Some(alias_only.clone()));
        assert_eq!(rt.get_node(&alias_only)?, None);

        let second = rt.gc()?;
        assert_eq!(second.deleted_objects, 0);
        assert_eq!(second.deleted_outcomes, 0);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn imports_files_and_trees_as_content_values() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let source_dir = temp.join("src");
        let nested_dir = source_dir.join("nested");
        std::fs::create_dir_all(&nested_dir)?;
        std::fs::write(source_dir.join("main.txt"), b"hello")?;
        std::fs::write(nested_dir.join("lib.txt"), b"world")?;

        let rt = Runtime::new(temp.join("store"))?;
        let file = rt.import_file(source_dir.join("main.txt"))?;
        assert_eq!(rt.force_value(file)?.0, Value::Blob(b"hello".to_vec()));

        let tree = rt.import_tree(&source_dir)?;
        let Value::Tree(tree_value) = rt.force_value(tree)?.0 else {
            panic!("import_tree should return a tree value");
        };
        assert!(matches!(
            tree_value.entries.get("main.txt"),
            Some(TreeEntry::Blob(_))
        ));
        assert!(matches!(
            tree_value.entries.get("nested"),
            Some(TreeEntry::Tree(_))
        ));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn gc_traces_tree_entries() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let source_dir = temp.join("src");
        std::fs::create_dir_all(&source_dir)?;
        std::fs::write(source_dir.join("main.txt"), b"hello")?;

        let rt = Runtime::new(temp.join("store"))?;
        let tree = rt.import_tree(&source_dir)?;
        let Value::Tree(tree_value) = rt.force_value(tree.clone())?.0 else {
            panic!("import_tree should return a tree value");
        };
        let Some(TreeEntry::Blob(blob)) = tree_value.entries.get("main.txt") else {
            panic!("tree should contain imported blob");
        };
        let blob = blob.clone();

        rt.pin("demo.src", tree.clone())?;
        let plan = rt.gc_plan()?;
        assert!(plan.reachable_objects.contains(&tree));
        assert!(plan.reachable_objects.contains(&blob));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn tree_get_and_put_create_new_immutable_trees() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let source_dir = temp.join("src");
        std::fs::create_dir_all(&source_dir)?;
        std::fs::write(source_dir.join("main.txt"), b"hello")?;

        let rt = Runtime::new(temp.join("store"))?;
        let tree = rt.import_tree(&source_dir)?;
        let main = rt
            .tree_get(tree.clone(), "main.txt")?
            .expect("main.txt should exist");
        assert_eq!(rt.force_value(main)?.0, Value::Blob(b"hello".to_vec()));

        let generated = rt.blob(b"generated".to_vec())?;
        let updated = rt.tree_put(
            tree.clone(),
            "nested/generated.txt",
            TreeEntry::Blob(generated.clone()),
        )?;

        assert_eq!(rt.tree_get(tree, "nested/generated.txt")?, None);
        assert_eq!(
            rt.tree_get(updated, "nested/generated.txt")?,
            Some(generated.clone())
        );
        assert_eq!(
            rt.force_value(generated)?.0,
            Value::Blob(b"generated".to_vec())
        );

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn exports_blobs_and_trees_to_host_paths() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let rt = Runtime::new(temp.join("store"))?;

        let blob = rt.blob(b"hello export".to_vec())?;
        let blob_dest = temp.join("out.txt");
        rt.export(blob, &blob_dest)?;
        assert_eq!(std::fs::read(&blob_dest)?, b"hello export");

        let inner = rt.blob(b"nested".to_vec())?;
        let tree = rt.tree(BTreeMap::from([(
            "nested.txt".to_owned(),
            TreeEntry::Blob(inner),
        )]))?;
        let tree_dest = temp.join("tree-out");
        rt.export(tree, &tree_dest)?;
        assert_eq!(std::fs::read(tree_dest.join("nested.txt"))?, b"nested");

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn cells_update_derived_computations() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let mut rt = Runtime::new(&temp)?;
        rt.register("+", HostFn::pure(add_ints));

        let initial = rt.int(100)?;
        let cell = rt.cell_new(initial)?;
        let read = rt.read_cell(cell.clone())?;
        let one = rt.int(1)?;
        let derived = rt.call("+", vec![read.clone(), one])?;

        let first = rt.force(derived.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(first_hash) = first.outcome else {
            panic!("derived computation should succeed");
        };
        assert_eq!(rt.get_value(&first_hash)?, Value::Int(101));

        let second = rt.force(derived.clone())?;
        assert!(second.cache_hit);
        let Outcome::Success(second_hash) = second.outcome else {
            panic!("derived computation should succeed from cache");
        };
        assert_eq!(rt.get_value(&second_hash)?, Value::Int(101));
        let explanation = rt.explain(&derived)?;
        assert!(explanation.contains("observed cells:"));
        assert!(explanation.contains("@ 0"));

        let updated = rt.int(200)?;
        let version = rt.cell_set(&cell, updated)?;
        assert_eq!(version.index, 1);

        let third = rt.force(derived)?;
        assert!(!third.cache_hit);
        let Outcome::Success(third_hash) = third.outcome else {
            panic!("derived computation should recompute after cell update");
        };
        assert_eq!(rt.get_value(&third_hash)?, Value::Int(201));
        assert_eq!(rt.cell_current(&cell)?.unwrap().index, 1);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn cells_keep_current_values_reachable_for_gc() -> anyhow::Result<()> {
        let temp = temp_store()?;

        let rt = Runtime::new(&temp)?;
        let initial = rt.int(1)?;
        let cell = rt.cell_new(initial.clone())?;
        let updated = rt.int(2)?;
        rt.cell_set(&cell, updated.clone())?;

        let plan = rt.gc_plan()?;
        assert!(plan.reachable_objects.contains(&updated));
        assert!(plan.unreachable_objects.contains(&initial));

        let stats = rt.store_stats()?;
        assert_eq!(stats.cell_count, 1);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn hermetic_action_runs_with_declared_inputs_and_caches_output_tree() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let source_dir = temp.join("src");
        std::fs::create_dir_all(&source_dir)?;
        std::fs::write(source_dir.join("main.txt"), b"hello action")?;

        let rt = Runtime::new(temp.join("store"))?;
        let src = rt.import_tree(&source_dir)?;
        let cp = rt.import_tool("/bin/cp")?;
        let action = rt.action(ActionSpec {
            program: "/bin/cp".to_owned(),
            tool: cp,
            args: vec!["src/main.txt".to_owned(), "out.txt".to_owned()],
            env: BTreeMap::new(),
            platform: std::env::consts::OS.to_owned(),
            inputs: vec![crate::ActionInput {
                path: "src".to_owned(),
                hash: src,
            }],
            outputs: vec!["out.txt".to_owned()],
        })?;

        let first = rt.force(action.clone())?;
        assert!(!first.cache_hit);
        let Outcome::Success(output_tree_hash) = first.outcome else {
            panic!("action should succeed");
        };
        let Value::ActionResult {
            outputs,
            stdout,
            stderr,
        } = rt.get_value(&output_tree_hash)?
        else {
            panic!("action should produce an action result");
        };
        assert_eq!(rt.force_value(stdout)?.0, Value::Blob(Vec::new()));
        assert_eq!(rt.force_value(stderr)?.0, Value::Blob(Vec::new()));

        let Value::Tree(output_tree) = rt.force_value(outputs)?.0 else {
            panic!("action result should reference an output tree");
        };
        let Some(TreeEntry::Blob(out_blob)) = output_tree.entries.get("out.txt") else {
            panic!("output tree should contain out.txt");
        };
        assert_eq!(
            rt.force_value(out_blob.clone())?.0,
            Value::Blob(b"hello action".to_vec())
        );

        let second = rt.force(action)?;
        assert!(second.cache_hit);

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn hermetic_action_failure_has_trace_and_logs() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let rt = Runtime::new(temp.join("store"))?;
        let cp = rt.import_tool("/bin/cp")?;
        let action_expr = rt.action(ActionSpec {
            program: "/bin/cp".to_owned(),
            tool: cp,
            args: vec!["missing.txt".to_owned(), "out.txt".to_owned()],
            env: BTreeMap::new(),
            platform: std::env::consts::OS.to_owned(),
            inputs: vec![],
            outputs: vec!["out.txt".to_owned()],
        })?;
        let action = rt.thunk(action_expr.clone())?;

        let forced = rt.force(action.clone())?;
        let Outcome::Failure(failure) = forced.outcome else {
            panic!("action should fail");
        };
        assert_eq!(failure.trace.hashes(), &[action, action_expr]);
        let FailureKind::ActionFailed {
            program,
            status,
            stderr,
            ..
        } = failure.kind
        else {
            panic!("expected action failure");
        };
        assert_eq!(program, "/bin/cp");
        assert_ne!(status, "0");
        assert!(stderr.contains("missing.txt"));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }

    #[test]
    fn explain_includes_failure_trace_and_action_stderr() -> anyhow::Result<()> {
        let temp = temp_store()?;
        let rt = Runtime::new(temp.join("store"))?;
        let cp = rt.import_tool("/bin/cp")?;
        let action = rt.action(ActionSpec {
            program: "/bin/cp".to_owned(),
            tool: cp,
            args: vec!["missing.txt".to_owned(), "out.txt".to_owned()],
            env: BTreeMap::new(),
            platform: std::env::consts::OS.to_owned(),
            inputs: vec![],
            outputs: vec!["out.txt".to_owned()],
        })?;

        let forced = rt.force(action.clone())?;
        assert!(matches!(forced.outcome, Outcome::Failure(_)));
        let explanation = rt.explain(&action)?;
        assert!(explanation.contains("action /bin/cp"));
        assert!(explanation.contains("=> failure"));
        assert!(explanation.contains("stderr:"));
        assert!(explanation.contains("missing.txt"));

        std::fs::remove_dir_all(temp)?;
        Ok(())
    }
}
