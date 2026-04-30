use crate::encode;
use crate::{
    ActionInput, CachedOutcome, CellId, CellVersion, Hash, Node, Outcome, TreeEntry, Value,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Store {
    root: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub object_count: u64,
    pub outcome_count: u64,
    pub root_count: u64,
    pub alias_count: u64,
    pub cell_count: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcPlan {
    pub roots: BTreeMap<String, Hash>,
    pub reachable_objects: BTreeSet<Hash>,
    pub reachable_outcomes: BTreeSet<Hash>,
    pub unreachable_objects: BTreeSet<Hash>,
    pub unreachable_outcomes: BTreeSet<Hash>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub plan: GcPlan,
    pub deleted_objects: u64,
    pub deleted_outcomes: u64,
    pub deleted_bytes: u64,
}

impl Store {
    pub fn open(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))?;
        fs::create_dir_all(root.join("outcomes"))?;
        Ok(Self { root })
    }

    pub fn put_node(&self, node: &Node) -> anyhow::Result<Hash> {
        self.put_object(ObjectKind::Node, node)
    }

    pub fn get_node(&self, hash: &Hash) -> anyhow::Result<Option<Node>> {
        self.get_object(hash, ObjectKind::Node)
    }

    pub fn put_value(&self, value: &Value) -> anyhow::Result<Hash> {
        self.put_object(ObjectKind::Value, value)
    }

    pub fn get_value(&self, hash: &Hash) -> anyhow::Result<Option<Value>> {
        self.get_object(hash, ObjectKind::Value)
    }

    pub fn put_outcome(&self, node: &Hash, outcome: &CachedOutcome) -> anyhow::Result<()> {
        self.replace_at(&self.outcome_path(node), outcome)
    }

    pub fn get_outcome(&self, node: &Hash) -> anyhow::Result<Option<Outcome>> {
        Ok(self.get_cached_outcome(node)?.map(|cached| cached.outcome))
    }

    pub fn get_cached_outcome(&self, node: &Hash) -> anyhow::Result<Option<CachedOutcome>> {
        self.get_at(&self.outcome_path(node))
    }

    pub fn pin(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        let mut roots = self.load_roots()?;
        roots.pins.insert(name.into(), hash);
        self.save_roots(&roots)
    }

    pub fn unpin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        let mut roots = self.load_roots()?;
        let removed = roots.pins.remove(name);
        self.save_roots(&roots)?;
        Ok(removed)
    }

    pub fn resolve_pin(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        Ok(self.load_roots()?.pins.get(name).cloned())
    }

    pub fn pins(&self) -> anyhow::Result<BTreeMap<String, Hash>> {
        Ok(self.load_roots()?.pins)
    }

    pub fn alias(&self, name: impl Into<String>, hash: Hash) -> anyhow::Result<()> {
        let mut aliases = self.load_aliases()?;
        aliases.names.insert(name.into(), hash);
        self.save_aliases(&aliases)
    }

    pub fn unalias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        let mut aliases = self.load_aliases()?;
        let removed = aliases.names.remove(name);
        self.save_aliases(&aliases)?;
        Ok(removed)
    }

    pub fn resolve_alias(&self, name: &str) -> anyhow::Result<Option<Hash>> {
        Ok(self.load_aliases()?.names.get(name).cloned())
    }

    pub fn aliases(&self) -> anyhow::Result<BTreeMap<String, Hash>> {
        Ok(self.load_aliases()?.names)
    }

    pub fn cell_new(&self, initial: Hash) -> anyhow::Result<CellId> {
        let mut cells = self.load_cells()?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
            .to_le_bytes();
        let id = CellId(Hash::new("cell", &nonce).to_string());
        cells.versions.entry(id.clone()).or_insert_with(|| {
            vec![CellVersion {
                index: 0,
                value: initial,
            }]
        });
        self.save_cells(&cells)?;
        Ok(id)
    }

    pub fn cell_set(&self, id: &CellId, value: Hash) -> anyhow::Result<CellVersion> {
        let mut cells = self.load_cells()?;
        let Some(versions) = cells.versions.get_mut(id) else {
            anyhow::bail!("unknown cell {}", id.0);
        };
        let version = CellVersion {
            index: versions
                .last()
                .map(|version| version.index + 1)
                .unwrap_or(0),
            value,
        };
        versions.push(version.clone());
        self.save_cells(&cells)?;
        Ok(version)
    }

    pub fn cell_current(&self, id: &CellId) -> anyhow::Result<Option<CellVersion>> {
        Ok(self
            .load_cells()?
            .versions
            .get(id)
            .and_then(|versions| versions.last())
            .cloned())
    }

    pub fn cells(&self) -> anyhow::Result<BTreeMap<CellId, Vec<CellVersion>>> {
        Ok(self.load_cells()?.versions)
    }

    pub fn stats(&self) -> anyhow::Result<StoreStats> {
        let object_stats = dir_stats(&self.root.join("objects"))?;
        let outcome_stats = dir_stats(&self.root.join("outcomes"))?;
        let roots_bytes = file_len(&self.roots_path())?;
        let aliases_bytes = file_len(&self.aliases_path())?;
        let cells_bytes = file_len(&self.cells_path())?;
        let root_count = self.load_roots()?.pins.len() as u64;
        let alias_count = self.load_aliases()?.names.len() as u64;
        let cell_count = self.load_cells()?.versions.len() as u64;

        Ok(StoreStats {
            object_count: object_stats.files,
            outcome_count: outcome_stats.files,
            root_count,
            alias_count,
            cell_count,
            total_bytes: object_stats.bytes
                + outcome_stats.bytes
                + roots_bytes
                + aliases_bytes
                + cells_bytes,
        })
    }

    pub fn gc_plan(&self) -> anyhow::Result<GcPlan> {
        let roots = self.pins()?;
        let mut reachable_objects = BTreeSet::new();
        let mut reachable_outcomes = BTreeSet::new();

        for hash in roots.values() {
            self.mark_reachable_object(hash, &mut reachable_objects, &mut reachable_outcomes)?;
        }
        for versions in self.cells()?.values() {
            if let Some(version) = versions.last() {
                self.mark_reachable_object(
                    &version.value,
                    &mut reachable_objects,
                    &mut reachable_outcomes,
                )?;
            }
        }

        let all_objects = self.object_hashes()?;
        let all_outcomes = self.outcome_hashes()?;
        let unreachable_objects = all_objects
            .difference(&reachable_objects)
            .cloned()
            .collect::<BTreeSet<_>>();
        let unreachable_outcomes = all_outcomes
            .difference(&reachable_outcomes)
            .cloned()
            .collect::<BTreeSet<_>>();

        Ok(GcPlan {
            roots,
            reachable_objects,
            reachable_outcomes,
            unreachable_objects,
            unreachable_outcomes,
        })
    }

    pub fn gc(&self) -> anyhow::Result<GcReport> {
        let plan = self.gc_plan()?;
        let mut deleted_objects = 0;
        let mut deleted_outcomes = 0;
        let mut deleted_bytes = 0;

        for hash in &plan.unreachable_outcomes {
            let path = self.outcome_path(hash);
            if path.exists() {
                deleted_bytes += fs::metadata(&path)?.len();
                fs::remove_file(path)?;
                deleted_outcomes += 1;
            }
        }

        for hash in &plan.unreachable_objects {
            let path = self.object_path(hash);
            if path.exists() {
                deleted_bytes += fs::metadata(&path)?.len();
                fs::remove_file(path)?;
                deleted_objects += 1;
            }
        }

        Ok(GcReport {
            plan,
            deleted_objects,
            deleted_outcomes,
            deleted_bytes,
        })
    }

    fn put_object<T: Serialize>(&self, kind: ObjectKind, object: &T) -> anyhow::Result<Hash> {
        let payload = encode(object)?;
        let hash = Hash::new(kind.domain(), &payload);
        let envelope = StoredObject {
            kind,
            schema: 1,
            payload,
        };
        self.put_at(&self.object_path(&hash), &envelope)?;
        Ok(hash)
    }

    fn get_object<T: DeserializeOwned>(
        &self,
        hash: &Hash,
        expected: ObjectKind,
    ) -> anyhow::Result<Option<T>> {
        let Some(envelope) = self.get_at::<StoredObject>(&self.object_path(hash))? else {
            return Ok(None);
        };

        if envelope.kind != expected {
            anyhow::bail!(
                "object {hash} has kind {:?}, expected {:?}",
                envelope.kind,
                expected
            );
        }

        if envelope.schema != 1 {
            anyhow::bail!("object {hash} uses unsupported schema {}", envelope.schema);
        }

        Ok(Some(encode::from_slice(&envelope.payload)?))
    }

    fn get_object_envelope(&self, hash: &Hash) -> anyhow::Result<Option<StoredObject>> {
        self.get_at::<StoredObject>(&self.object_path(hash))
    }

    fn mark_reachable_object(
        &self,
        hash: &Hash,
        reachable_objects: &mut BTreeSet<Hash>,
        reachable_outcomes: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<()> {
        if !reachable_objects.insert(hash.clone()) {
            return Ok(());
        }

        let Some(envelope) = self.get_object_envelope(hash)? else {
            return Ok(());
        };

        if envelope.schema != 1 {
            anyhow::bail!("object {hash} uses unsupported schema {}", envelope.schema);
        }

        match envelope.kind {
            ObjectKind::Node => {
                let node = encode::from_slice::<Node>(&envelope.payload)?;
                self.mark_node_references(&node, reachable_objects, reachable_outcomes)?;
                if let Some(outcome) = self.get_outcome(hash)? {
                    reachable_outcomes.insert(hash.clone());
                    self.mark_outcome_references(&outcome, reachable_objects, reachable_outcomes)?;
                }
            }
            ObjectKind::Value => {
                let value = encode::from_slice::<Value>(&envelope.payload)?;
                self.mark_value_references(&value, reachable_objects, reachable_outcomes)?;
            }
        }

        Ok(())
    }

    fn mark_node_references(
        &self,
        node: &Node,
        reachable_objects: &mut BTreeSet<Hash>,
        reachable_outcomes: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<()> {
        match node {
            Node::Value(value) => {
                self.mark_value_references(value, reachable_objects, reachable_outcomes)?;
            }
            Node::Thunk { target } => {
                self.mark_reachable_object(target, reachable_objects, reachable_outcomes)?;
            }
            Node::Apply { args, .. } | Node::HostCall { args, .. } => {
                for arg in args {
                    self.mark_reachable_object(arg, reachable_objects, reachable_outcomes)?;
                }
            }
            Node::Action(spec) => {
                self.mark_reachable_object(&spec.tool, reachable_objects, reachable_outcomes)?;
                for ActionInput { hash, .. } in &spec.inputs {
                    self.mark_reachable_object(hash, reachable_objects, reachable_outcomes)?;
                }
            }
            Node::ReadCell(cell) => {
                if let Some(version) = self.cell_current(cell)? {
                    self.mark_reachable_object(
                        &version.value,
                        reachable_objects,
                        reachable_outcomes,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn mark_value_references(
        &self,
        value: &Value,
        reachable_objects: &mut BTreeSet<Hash>,
        reachable_outcomes: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<()> {
        match value {
            Value::Int(_) | Value::Text(_) | Value::Bytes(_) | Value::Blob(_) => {}
            Value::Tree(tree) => {
                for entry in tree.entries.values() {
                    match entry {
                        TreeEntry::Blob(hash) | TreeEntry::Tree(hash) => {
                            self.mark_reachable_object(
                                hash,
                                reachable_objects,
                                reachable_outcomes,
                            )?;
                        }
                    }
                }
            }
            Value::Tuple(items) => {
                for item in items {
                    self.mark_reachable_object(item, reachable_objects, reachable_outcomes)?;
                }
            }
            Value::Artifact(hash) => {
                self.mark_reachable_object(hash, reachable_objects, reachable_outcomes)?;
            }
            Value::ActionResult {
                outputs,
                stdout,
                stderr,
            } => {
                self.mark_reachable_object(outputs, reachable_objects, reachable_outcomes)?;
                self.mark_reachable_object(stdout, reachable_objects, reachable_outcomes)?;
                self.mark_reachable_object(stderr, reachable_objects, reachable_outcomes)?;
            }
        }

        Ok(())
    }

    fn mark_outcome_references(
        &self,
        outcome: &Outcome,
        reachable_objects: &mut BTreeSet<Hash>,
        reachable_outcomes: &mut BTreeSet<Hash>,
    ) -> anyhow::Result<()> {
        match outcome {
            Outcome::Success(hash) => {
                self.mark_reachable_object(hash, reachable_objects, reachable_outcomes)?;
            }
            Outcome::Failure(_) => {}
        }

        Ok(())
    }

    fn put_at<T: Serialize>(&self, path: &Path, object: &T) -> anyhow::Result<()> {
        let bytes = encode(object)?;
        self.put_bytes_if_absent(path, &bytes)
    }

    fn get_at<T: DeserializeOwned>(&self, path: &Path) -> anyhow::Result<Option<T>> {
        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path)?;
        Ok(Some(encode::from_slice(&bytes)?))
    }

    fn put_bytes_if_absent(&self, path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        if path.exists() {
            return Ok(());
        }

        write_bytes_atomic(path, bytes)
    }

    fn replace_at<T: Serialize>(&self, path: &Path, object: &T) -> anyhow::Result<()> {
        let bytes = encode(object)?;
        write_bytes_atomic(path, &bytes)
    }

    fn object_path(&self, hash: &Hash) -> PathBuf {
        self.root
            .join("objects")
            .join(hash.shard())
            .join(format!("{}.r2obj", hash.body()))
    }

    fn outcome_path(&self, hash: &Hash) -> PathBuf {
        self.root
            .join("outcomes")
            .join(hash.shard())
            .join(format!("{}.r2out", hash.body()))
    }

    fn roots_path(&self) -> PathBuf {
        self.root.join("roots.r2")
    }

    fn aliases_path(&self) -> PathBuf {
        self.root.join("aliases.r2")
    }

    fn cells_path(&self) -> PathBuf {
        self.root.join("cells.r2")
    }

    fn object_hashes(&self) -> anyhow::Result<BTreeSet<Hash>> {
        collect_hashes(&self.root.join("objects"), "r2obj")
    }

    fn outcome_hashes(&self) -> anyhow::Result<BTreeSet<Hash>> {
        collect_hashes(&self.root.join("outcomes"), "r2out")
    }

    fn load_roots(&self) -> anyhow::Result<RootsFile> {
        Ok(self.get_at(&self.roots_path())?.unwrap_or_default())
    }

    fn save_roots(&self, roots: &RootsFile) -> anyhow::Result<()> {
        self.replace_at(&self.roots_path(), roots)
    }

    fn load_aliases(&self) -> anyhow::Result<AliasesFile> {
        Ok(self.get_at(&self.aliases_path())?.unwrap_or_default())
    }

    fn save_aliases(&self, aliases: &AliasesFile) -> anyhow::Result<()> {
        self.replace_at(&self.aliases_path(), aliases)
    }

    fn load_cells(&self) -> anyhow::Result<CellsFile> {
        Ok(self.get_at(&self.cells_path())?.unwrap_or_default())
    }

    fn save_cells(&self, cells: &CellsFile) -> anyhow::Result<()> {
        self.replace_at(&self.cells_path(), cells)
    }
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn encode<T: Serialize>(object: &T) -> anyhow::Result<Vec<u8>> {
    crate::encode::to_canonical_vec(object)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum ObjectKind {
    Node,
    Value,
}

impl ObjectKind {
    fn domain(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Value => "value",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredObject {
    kind: ObjectKind,
    schema: u32,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RootsFile {
    schema: u32,
    pins: BTreeMap<String, Hash>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct AliasesFile {
    schema: u32,
    names: BTreeMap<String, Hash>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct CellsFile {
    schema: u32,
    versions: BTreeMap<CellId, Vec<CellVersion>>,
}

#[derive(Default)]
struct DirStats {
    files: u64,
    bytes: u64,
}

fn dir_stats(path: &Path) -> anyhow::Result<DirStats> {
    if !path.exists() {
        return Ok(DirStats::default());
    }

    let mut stats = DirStats::default();
    accumulate_dir_stats(path, &mut stats)?;
    Ok(stats)
}

fn accumulate_dir_stats(path: &Path, stats: &mut DirStats) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            accumulate_dir_stats(&entry.path(), stats)?;
        } else if metadata.is_file() {
            stats.files += 1;
            stats.bytes += metadata.len();
        }
    }

    Ok(())
}

fn file_len(path: &Path) -> anyhow::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }

    Ok(fs::metadata(path)?.len())
}

fn collect_hashes(path: &Path, extension: &str) -> anyhow::Result<BTreeSet<Hash>> {
    let mut hashes = BTreeSet::new();
    if !path.exists() {
        return Ok(hashes);
    }

    collect_hashes_inner(path, extension, &mut hashes)?;
    Ok(hashes)
}

fn collect_hashes_inner(
    path: &Path,
    extension: &str,
    hashes: &mut BTreeSet<Hash>,
) -> anyhow::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let path = entry.path();

        if metadata.is_dir() {
            collect_hashes_inner(&path, extension, hashes)?;
        } else if metadata.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some(extension)
            && let Some(stem) = path.file_stem().and_then(|value| value.to_str())
        {
            hashes.insert(Hash::from_str(&format!("sha256:{stem}"))?);
        }
    }

    Ok(())
}
