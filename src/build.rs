use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::effects::process;
use crate::{RuntimeValue, Symbol, Term, Value, thunk};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Artifact {
    path: Vec<u8>,
}

impl Artifact {
    pub fn new(path: impl Into<Vec<u8>>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &[u8] {
        &self.path
    }

    pub fn into_path(self) -> Vec<u8> {
        self.path
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    argv: Vec<Vec<u8>>,
    env_mode: process::EnvMode,
    env: BTreeMap<Symbol, Vec<u8>>,
    cwd: Option<Vec<u8>>,
    stdin: Vec<u8>,
    inputs: Vec<Artifact>,
    outputs: Vec<Artifact>,
}

impl Action {
    pub fn new(argv: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            argv: argv.into_iter().collect(),
            env_mode: process::EnvMode::Clear,
            env: BTreeMap::new(),
            cwd: None,
            stdin: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    pub fn argv(&self) -> &[Vec<u8>] {
        &self.argv
    }

    pub fn inputs(&self) -> &[Artifact] {
        &self.inputs
    }

    pub fn outputs(&self) -> &[Artifact] {
        &self.outputs
    }

    pub fn inherit_env(mut self) -> Self {
        self.env_mode = process::EnvMode::Inherit;
        self
    }

    pub fn cwd(mut self, cwd: impl Into<Vec<u8>>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn stdin(mut self, stdin: impl Into<Vec<u8>>) -> Self {
        self.stdin = stdin.into();
        self
    }

    pub fn env(mut self, name: impl Into<Symbol>, value: impl Into<Vec<u8>>) -> Self {
        self.env.insert(name.into(), value.into());
        self
    }

    pub fn input(mut self, artifact: Artifact) -> Self {
        self.inputs.push(artifact);
        self
    }

    pub fn output(mut self, artifact: Artifact) -> Self {
        self.outputs.push(artifact);
        self
    }

    pub fn to_spawn_request(&self) -> process::SpawnRequest {
        let mut request = process::SpawnRequest::new(self.argv.clone());
        if self.env_mode == process::EnvMode::Inherit {
            request = request.inherit_env();
        }
        if let Some(cwd) = &self.cwd {
            request = request.cwd(cwd.clone());
        }
        if !self.stdin.is_empty() {
            request = request.stdin(self.stdin.clone());
        }
        for (name, value) in &self.env {
            request = request.env(name.clone(), value.clone());
        }
        for artifact in &self.inputs {
            request = request.declared_input(artifact.path.clone());
        }
        for artifact in &self.outputs {
            request = request.declared_output(artifact.path.clone());
        }
        request
    }

    pub fn to_value(&self) -> Value {
        self.to_spawn_request().to_value()
    }

    pub fn into_term(self) -> Term {
        self.to_spawn_request().into_term()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Handle(usize);

impl Handle {
    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphNode {
    Input {
        artifact: Artifact,
    },
    Action {
        action: Action,
        dependencies: Vec<Handle>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphError {
    UnknownHandle(Handle),
    DuplicateTarget(Symbol),
    Cycle { handle: Handle },
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownHandle(handle) => write!(f, "unknown build graph handle {}", handle.0),
            Self::DuplicateTarget(target) => write!(f, "duplicate build graph target {target}"),
            Self::Cycle { handle } => write!(f, "build graph cycle at handle {}", handle.0),
        }
    }
}

impl std::error::Error for GraphError {}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Graph {
    nodes: Vec<GraphNode>,
    artifact_producers: BTreeMap<Vec<u8>, Handle>,
    targets: BTreeMap<Symbol, Handle>,
}

impl Graph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn input(&mut self, path: impl Into<Vec<u8>>) -> Handle {
        let artifact = Artifact::new(path);
        let handle = self.push_node(GraphNode::Input {
            artifact: artifact.clone(),
        });
        self.artifact_producers
            .insert(artifact.path().to_vec(), handle);
        handle
    }

    pub fn action(&mut self, action: Action) -> Handle {
        let dependencies = self.dependencies_for_action(&action);
        let outputs = action
            .outputs()
            .iter()
            .map(|artifact| artifact.path().to_vec())
            .collect::<Vec<_>>();
        let handle = self.push_node(GraphNode::Action {
            action,
            dependencies,
        });
        for output in outputs {
            self.artifact_producers.insert(output, handle);
        }
        handle
    }

    pub fn target(&mut self, name: impl Into<Symbol>, handle: Handle) -> Result<(), GraphError> {
        self.require_handle(handle)?;
        let name = name.into();
        if self.targets.contains_key(&name) {
            return Err(GraphError::DuplicateTarget(name));
        }
        self.targets.insert(name, handle);
        Ok(())
    }

    pub fn node(&self, handle: Handle) -> Option<&GraphNode> {
        self.nodes.get(handle.0)
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn dependencies_of(&self, handle: Handle) -> Result<Vec<Handle>, GraphError> {
        self.require_handle(handle)?;
        let mut visited = BTreeSet::new();
        let mut dependencies = Vec::new();
        self.collect_dependencies(handle, &mut visited, &mut dependencies)?;
        Ok(dependencies)
    }

    pub fn reverse_dependencies_of(&self, handle: Handle) -> Result<Vec<Handle>, GraphError> {
        self.require_handle(handle)?;
        let mut visited = BTreeSet::new();
        let mut reverse = Vec::new();
        self.collect_reverse_dependencies(handle, &mut visited, &mut reverse)?;
        Ok(reverse)
    }

    pub fn topological_order(&self) -> Result<Vec<Handle>, GraphError> {
        let mut temporary = BTreeSet::new();
        let mut permanent = BTreeSet::new();
        let mut order = Vec::new();
        for index in 0..self.nodes.len() {
            self.visit_topological(Handle(index), &mut temporary, &mut permanent, &mut order)?;
        }
        Ok(order)
    }

    pub fn topological_layers(&self) -> Result<Vec<Vec<Handle>>, GraphError> {
        let order = self.topological_order()?;
        let mut depths = BTreeMap::new();
        for handle in order {
            let depth = self
                .direct_dependencies(handle)?
                .iter()
                .map(|dependency| depths.get(dependency).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0);
            depths.insert(handle, depth);
        }

        let mut layers = Vec::<Vec<Handle>>::new();
        for (handle, depth) in depths {
            if layers.len() <= depth {
                layers.resize_with(depth + 1, Vec::new);
            }
            layers[depth].push(handle);
        }

        Ok(layers)
    }

    pub fn to_term(&self) -> Result<Term, GraphError> {
        for handle in self.targets.values() {
            self.require_acyclic(*handle)?;
        }

        if self.targets.is_empty() {
            return Ok(Term::Record(BTreeMap::new()));
        }

        Ok(Term::Record(
            self.targets
                .iter()
                .map(|(name, handle)| (name.clone(), self.term_for_handle(*handle)))
                .collect(),
        ))
    }

    pub fn render_dot(&self) -> String {
        let mut dot = String::from("digraph build {\n  rankdir=LR;\n");
        for (index, node) in self.nodes.iter().enumerate() {
            dot.push_str(&format!(
                "  n{index} [label=\"{}\"];\n",
                escape_dot_label(&node_label(node, index))
            ));
        }
        for (index, node) in self.nodes.iter().enumerate() {
            if let GraphNode::Action { dependencies, .. } = node {
                for dependency in dependencies {
                    dot.push_str(&format!("  n{} -> n{index};\n", dependency.0));
                }
            }
        }
        for (name, handle) in &self.targets {
            let target_id = format!("target_{}", sanitize_dot_id(name.as_str()));
            dot.push_str(&format!(
                "  {target_id} [shape=box,label=\"target:{}\"];\n",
                escape_dot_label(name.as_str())
            ));
            dot.push_str(&format!("  n{} -> {target_id};\n", handle.0));
        }
        dot.push_str("}\n");
        dot
    }

    fn push_node(&mut self, node: GraphNode) -> Handle {
        let handle = Handle(self.nodes.len());
        self.nodes.push(node);
        handle
    }

    fn dependencies_for_action(&self, action: &Action) -> Vec<Handle> {
        let mut seen = BTreeSet::new();
        let mut dependencies = Vec::new();
        for input in action.inputs() {
            if let Some(handle) = self.artifact_producers.get(input.path())
                && seen.insert(*handle)
            {
                dependencies.push(*handle);
            }
        }
        dependencies
    }

    fn direct_dependencies(&self, handle: Handle) -> Result<&[Handle], GraphError> {
        match self.node(handle).ok_or(GraphError::UnknownHandle(handle))? {
            GraphNode::Input { .. } => Ok(&[]),
            GraphNode::Action { dependencies, .. } => Ok(dependencies),
        }
    }

    fn collect_dependencies(
        &self,
        handle: Handle,
        visited: &mut BTreeSet<Handle>,
        dependencies: &mut Vec<Handle>,
    ) -> Result<(), GraphError> {
        for dependency in self.direct_dependencies(handle)? {
            if visited.insert(*dependency) {
                self.collect_dependencies(*dependency, visited, dependencies)?;
                dependencies.push(*dependency);
            }
        }
        Ok(())
    }

    fn collect_reverse_dependencies(
        &self,
        handle: Handle,
        visited: &mut BTreeSet<Handle>,
        reverse: &mut Vec<Handle>,
    ) -> Result<(), GraphError> {
        for (index, node) in self.nodes.iter().enumerate() {
            let candidate = Handle(index);
            let GraphNode::Action { dependencies, .. } = node else {
                continue;
            };
            if dependencies.contains(&handle) && visited.insert(candidate) {
                reverse.push(candidate);
                self.collect_reverse_dependencies(candidate, visited, reverse)?;
            }
        }
        Ok(())
    }

    fn visit_topological(
        &self,
        handle: Handle,
        temporary: &mut BTreeSet<Handle>,
        permanent: &mut BTreeSet<Handle>,
        order: &mut Vec<Handle>,
    ) -> Result<(), GraphError> {
        self.require_handle(handle)?;
        if permanent.contains(&handle) {
            return Ok(());
        }
        if !temporary.insert(handle) {
            return Err(GraphError::Cycle { handle });
        }
        for dependency in self.direct_dependencies(handle)? {
            self.visit_topological(*dependency, temporary, permanent, order)?;
        }
        temporary.remove(&handle);
        permanent.insert(handle);
        order.push(handle);
        Ok(())
    }

    fn require_handle(&self, handle: Handle) -> Result<(), GraphError> {
        if handle.0 < self.nodes.len() {
            Ok(())
        } else {
            Err(GraphError::UnknownHandle(handle))
        }
    }

    fn require_acyclic(&self, handle: Handle) -> Result<(), GraphError> {
        let mut temporary = BTreeSet::new();
        let mut permanent = BTreeSet::new();
        let mut order = Vec::new();
        self.visit_topological(handle, &mut temporary, &mut permanent, &mut order)
    }

    fn term_for_handle(&self, handle: Handle) -> Term {
        match &self.nodes[handle.0] {
            GraphNode::Input { artifact } => Term::Value(Value::Bytes(artifact.path().to_vec())),
            GraphNode::Action { .. } => thunk::force(self.thunk_for_handle(handle)),
        }
    }

    fn thunk_for_handle(&self, handle: Handle) -> Term {
        match &self.nodes[handle.0] {
            GraphNode::Input { artifact } => {
                thunk::delay(Term::Value(Value::Bytes(artifact.path().to_vec())))
            }
            GraphNode::Action {
                action,
                dependencies,
            } => {
                let dependency_thunks = dependencies
                    .iter()
                    .filter_map(|dependency| match &self.nodes[dependency.0] {
                        GraphNode::Input { .. } => None,
                        GraphNode::Action { .. } => Some(self.thunk_for_handle(*dependency)),
                    })
                    .collect::<Vec<_>>();
                let prerequisites = if dependency_thunks.is_empty() {
                    Vec::new()
                } else {
                    vec![thunk::force_all(dependency_thunks)]
                };
                thunk::delay(sequence_terms(prerequisites, action.clone().into_term()))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    ExitCode(i64),
    Signal(i64),
    Unknown,
}

impl Status {
    pub fn succeeded(&self) -> bool {
        matches!(self, Self::ExitCode(0))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedArtifact {
    pub artifact: Artifact,
    pub contents: Result<Vec<u8>, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinishedAction {
    pub status: Status,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub inputs: Vec<Artifact>,
    pub outputs: Vec<MaterializedArtifact>,
}

impl FinishedAction {
    pub fn succeeded(&self) -> bool {
        self.status.succeeded()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResultValue {
    Finished(FinishedAction),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    ExpectedRuntimeData,
    Process(process::DecodeError),
    ErrorPayloadShape,
    OutputFileCountMismatch {
        declared: usize,
        materialized: usize,
    },
    OutputPathMismatch {
        index: usize,
        declared: Vec<u8>,
        materialized: Vec<u8>,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedRuntimeData => f.write_str("build result must be a data value"),
            Self::Process(error) => error.fmt(f),
            Self::ErrorPayloadShape => f.write_str("build error result must be error(bytes)"),
            Self::OutputFileCountMismatch {
                declared,
                materialized,
            } => {
                write!(
                    f,
                    "build result declared {declared} outputs but materialized {materialized}"
                )
            }
            Self::OutputPathMismatch {
                index,
                declared,
                materialized,
            } => write!(
                f,
                "build result output {index} had path {:?}, expected {:?}",
                materialized, declared
            ),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<process::DecodeError> for DecodeError {
    fn from(value: process::DecodeError) -> Self {
        Self::Process(value)
    }
}

pub fn decode_runtime_value(value: &RuntimeValue) -> Result<ResultValue, DecodeError> {
    match value {
        RuntimeValue::Data(value) => decode_value(value),
        _ => Err(DecodeError::ExpectedRuntimeData),
    }
}

pub fn decode_value(value: &Value) -> Result<ResultValue, DecodeError> {
    if let Value::Tagged { tag, fields } = value
        && tag.as_str() == "error"
    {
        return decode_error(fields);
    }

    let decoded = process::decode_result(value)?;
    Ok(ResultValue::Finished(FinishedAction {
        status: decode_status(decoded.status),
        stdout: decoded.stdout,
        stderr: decoded.stderr,
        inputs: decoded
            .declared_inputs
            .into_iter()
            .map(Artifact::new)
            .collect(),
        outputs: zip_outputs(decoded.declared_outputs, decoded.output_files)?,
    }))
}

fn decode_error(fields: &[Value]) -> Result<ResultValue, DecodeError> {
    match fields {
        [Value::Bytes(bytes)] => Ok(ResultValue::Error(
            String::from_utf8_lossy(bytes).into_owned(),
        )),
        _ => Err(DecodeError::ErrorPayloadShape),
    }
}

fn decode_status(status: process::ProcessStatus) -> Status {
    match status {
        process::ProcessStatus::ExitCode(code) => Status::ExitCode(code),
        process::ProcessStatus::Signal(signal) => Status::Signal(signal),
        process::ProcessStatus::Unknown => Status::Unknown,
    }
}

fn zip_outputs(
    declared_outputs: Vec<Vec<u8>>,
    output_files: Vec<process::DeclaredOutputFile>,
) -> Result<Vec<MaterializedArtifact>, DecodeError> {
    if declared_outputs.len() != output_files.len() {
        return Err(DecodeError::OutputFileCountMismatch {
            declared: declared_outputs.len(),
            materialized: output_files.len(),
        });
    }

    let mut outputs = Vec::with_capacity(declared_outputs.len());
    for (index, (declared, materialized)) in declared_outputs
        .into_iter()
        .zip(output_files.into_iter())
        .enumerate()
    {
        if declared != materialized.path {
            return Err(DecodeError::OutputPathMismatch {
                index,
                declared,
                materialized: materialized.path,
            });
        }
        outputs.push(MaterializedArtifact {
            artifact: Artifact::new(declared),
            contents: materialized.contents,
        });
    }

    Ok(outputs)
}

fn sequence_terms(prerequisites: Vec<Term>, body: Term) -> Term {
    prerequisites
        .into_iter()
        .rev()
        .fold(body, |body, prerequisite| Term::Apply {
            callee: Box::new(Term::lambda(1, body)),
            args: vec![prerequisite],
        })
}

fn node_label(node: &GraphNode, index: usize) -> String {
    match node {
        GraphNode::Input { artifact } => {
            format!(
                "input#{index}\\n{}",
                String::from_utf8_lossy(artifact.path())
            )
        }
        GraphNode::Action { action, .. } => {
            let outputs = action
                .outputs()
                .iter()
                .map(|artifact| String::from_utf8_lossy(artifact.path()).into_owned())
                .collect::<Vec<_>>()
                .join(", ");
            if outputs.is_empty() {
                format!("action#{index}")
            } else {
                format!("action#{index}\\n{outputs}")
            }
        }
    }
}

fn escape_dot_label(label: &str) -> String {
    label.replace('\\', "\\\\").replace('"', "\\\"")
}

fn sanitize_dot_id(value: &str) -> String {
    let mut output = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            output.push(ch);
        } else {
            output.push('_');
        }
    }
    if output.is_empty() {
        output.push_str("unnamed");
    }
    output
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    type OutputFixture = (Vec<u8>, Result<Vec<u8>, String>);

    #[test]
    fn action_builds_process_request_with_explicit_artifacts() {
        let action = Action::new(vec![b"/bin/tool".to_vec(), b"--flag".to_vec()])
            .inherit_env()
            .cwd(b"/work".to_vec())
            .stdin(b"hello".to_vec())
            .env("HOME", b"/tmp".to_vec())
            .input(Artifact::new(b"/src/input.txt".to_vec()))
            .output(Artifact::new(b"/out/artifact.txt".to_vec()));

        let request = action.to_spawn_request();

        assert_eq!(
            request.argv,
            vec![b"/bin/tool".to_vec(), b"--flag".to_vec()]
        );
        assert_eq!(request.env_mode, process::EnvMode::Inherit);
        assert_eq!(
            request.env.get(&Symbol::from("HOME")),
            Some(&b"/tmp".to_vec())
        );
        assert_eq!(request.cwd, Some(b"/work".to_vec()));
        assert_eq!(request.stdin, b"hello".to_vec());
        assert_eq!(request.declared_inputs, vec![b"/src/input.txt".to_vec()]);
        assert_eq!(
            request.declared_outputs,
            vec![b"/out/artifact.txt".to_vec()]
        );
    }

    #[test]
    fn decode_value_maps_finished_build_outputs() {
        let value = ok_result(
            process::ProcessStatus::ExitCode(0),
            vec![b"/src/input.txt".to_vec()],
            vec![(b"/out/artifact.txt".to_vec(), Ok(b"artifact".to_vec()))],
        );

        let decoded = decode_value(&value).expect("build result should decode");

        assert_eq!(
            decoded,
            ResultValue::Finished(FinishedAction {
                status: Status::ExitCode(0),
                stdout: b"stdout".to_vec(),
                stderr: b"stderr".to_vec(),
                inputs: vec![Artifact::new(b"/src/input.txt".to_vec())],
                outputs: vec![MaterializedArtifact {
                    artifact: Artifact::new(b"/out/artifact.txt".to_vec()),
                    contents: Ok(b"artifact".to_vec()),
                }],
            })
        );
    }

    #[test]
    fn decode_value_maps_error_results() {
        let value = Value::Tagged {
            tag: Symbol::from("error"),
            fields: vec![Value::Bytes(b"spawn failed".to_vec())],
        };

        let decoded = decode_value(&value).expect("error result should decode");

        assert_eq!(decoded, ResultValue::Error("spawn failed".to_string()));
    }

    #[test]
    fn decode_value_rejects_output_path_mismatches() {
        let value = ok_result(
            process::ProcessStatus::ExitCode(0),
            Vec::new(),
            vec![(b"/out/actual.txt".to_vec(), Ok(Vec::new()))],
        );

        let Value::Tagged { fields, .. } = value else {
            panic!("expected tagged result");
        };
        let Value::Record(record) = &fields[0] else {
            panic!("expected record payload");
        };
        let mut record = record.clone();
        record.insert(
            Symbol::from("declared_outputs"),
            Value::List(vec![Value::Bytes(b"/out/expected.txt".to_vec())]),
        );
        let value = Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(record)],
        };

        let error = decode_value(&value).expect_err("mismatched outputs should fail");

        assert_eq!(
            error,
            DecodeError::OutputPathMismatch {
                index: 0,
                declared: b"/out/expected.txt".to_vec(),
                materialized: b"/out/actual.txt".to_vec(),
            }
        );
    }

    fn ok_result(
        status: process::ProcessStatus,
        declared_inputs: Vec<Vec<u8>>,
        outputs: Vec<OutputFixture>,
    ) -> Value {
        let declared_outputs = outputs
            .iter()
            .map(|(path, _)| Value::Bytes(path.clone()))
            .collect();
        let output_files = outputs
            .into_iter()
            .map(|(path, contents)| {
                Value::Record(BTreeMap::from([
                    (Symbol::from("path"), Value::Bytes(path)),
                    (
                        Symbol::from("contents"),
                        match contents {
                            Ok(bytes) => Value::Tagged {
                                tag: Symbol::from("ok"),
                                fields: vec![Value::Bytes(bytes)],
                            },
                            Err(message) => Value::Tagged {
                                tag: Symbol::from("error"),
                                fields: vec![Value::Bytes(message.into_bytes())],
                            },
                        },
                    ),
                ]))
            })
            .collect();

        Value::Tagged {
            tag: Symbol::from("ok"),
            fields: vec![Value::Record(BTreeMap::from([
                (
                    Symbol::from("status"),
                    match status {
                        process::ProcessStatus::ExitCode(code) => Value::Tagged {
                            tag: Symbol::from("exit_code"),
                            fields: vec![Value::Integer(code)],
                        },
                        process::ProcessStatus::Signal(signal) => Value::Tagged {
                            tag: Symbol::from("signal"),
                            fields: vec![Value::Integer(signal)],
                        },
                        process::ProcessStatus::Unknown => Value::Tagged {
                            tag: Symbol::from("unknown_status"),
                            fields: Vec::new(),
                        },
                    },
                ),
                (Symbol::from("stdout"), Value::Bytes(b"stdout".to_vec())),
                (Symbol::from("stderr"), Value::Bytes(b"stderr".to_vec())),
                (
                    Symbol::from("declared_inputs"),
                    Value::List(declared_inputs.into_iter().map(Value::Bytes).collect()),
                ),
                (
                    Symbol::from("declared_outputs"),
                    Value::List(declared_outputs),
                ),
                (Symbol::from("output_files"), Value::List(output_files)),
            ]))],
        }
    }
}
