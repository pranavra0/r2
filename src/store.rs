use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::{Canonical, Digest, Ref, Term, Value};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);
static ACCESS_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Stored {
    Term(Term),
    Value(Value),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "ref", rename_all = "snake_case")]
pub enum CachedThunk {
    Value(Ref),
    Lambda(Ref),
    Ref(Ref),
}

impl Stored {
    pub fn term(term: Term) -> Self {
        Self::Term(term)
    }

    pub fn value(value: Value) -> Self {
        Self::Value(value)
    }

    fn references(&self) -> Vec<Ref> {
        let mut references = Vec::new();
        if let Self::Term(term) = self {
            collect_term_refs(term, &mut references);
        }
        references
    }
}

impl Canonical for Stored {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        match self {
            Self::Term(term) => {
                out.push(0x20);
                term.write_canonical(out);
            }
            Self::Value(value) => {
                out.push(0x21);
                value.write_canonical(out);
            }
        }
    }
}

#[derive(Debug)]
pub enum StoreError {
    OpenTerm,
    MissingRef(Ref),
    Io {
        op: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Encode(serde_json::Error),
    Decode {
        path: PathBuf,
        source: serde_json::Error,
    },
    CacheDecode {
        path: PathBuf,
        source: serde_json::Error,
    },
    HashMismatch {
        path: PathBuf,
        expected: Digest,
        actual: Digest,
    },
    InvalidObjectPath {
        path: PathBuf,
    },
}

impl StoreError {
    fn io(op: &'static str, path: PathBuf, source: io::Error) -> Self {
        Self::Io { op, path, source }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenTerm => f.write_str("store only accepts closed terms"),
            Self::MissingRef(reference) => write!(f, "missing store entry {}", reference.hash),
            Self::Io { op, path, source } => {
                write!(f, "failed to {op} {}: {source}", path.display())
            }
            Self::Encode(source) => write!(f, "failed to encode stored object: {source}"),
            Self::Decode { path, source } => {
                write!(
                    f,
                    "failed to decode store object {}: {source}",
                    path.display()
                )
            }
            Self::CacheDecode { path, source } => {
                write!(
                    f,
                    "failed to decode thunk cache entry {}: {source}",
                    path.display()
                )
            }
            Self::HashMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "store object {} had digest {actual}, expected {expected}",
                path.display()
            ),
            Self::InvalidObjectPath { path } => {
                write!(f, "store object path {} was not a digest", path.display())
            }
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Encode(source) => Some(source),
            Self::Decode { source, .. } => Some(source),
            Self::CacheDecode { source, .. } => Some(source),
            Self::OpenTerm
            | Self::MissingRef(_)
            | Self::HashMismatch { .. }
            | Self::InvalidObjectPath { .. } => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub reachable: usize,
    pub deleted_objects: usize,
    pub kept_objects: usize,
    pub deleted_cache_entries: usize,
}

pub trait ObjectStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError>;
    fn get(&self, reference: &Ref) -> Result<Option<Stored>, StoreError>;

    fn put_cached_thunk(&mut self, _key: Digest, _cached: CachedThunk) -> Result<(), StoreError> {
        Ok(())
    }

    fn get_cached_thunk(&self, _key: &Digest) -> Result<Option<CachedThunk>, StoreError> {
        Ok(None)
    }

    fn load(&self, reference: &Ref) -> Result<Stored, StoreError> {
        self.get(reference)
            .and_then(|stored| stored.ok_or_else(|| StoreError::MissingRef(reference.clone())))
    }
}

#[derive(Clone, Debug)]
pub struct FileStore {
    root: PathBuf,
    max_size_bytes: Option<u64>,
}

impl FileStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let store = Self {
            root: root.into(),
            max_size_bytes: None,
        };
        let objects_dir = store.objects_dir();
        fs::create_dir_all(&objects_dir)
            .map_err(|source| StoreError::io("create store directory", objects_dir, source))?;
        Ok(store)
    }

    pub fn with_max_size(mut self, bytes: u64) -> Self {
        self.max_size_bytes = Some(bytes);
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn objects_dir(&self) -> PathBuf {
        self.root.join("objects")
    }

    fn thunk_cache_dir(&self) -> PathBuf {
        self.root.join("cache").join("thunks")
    }

    fn access_dir(&self) -> PathBuf {
        self.root.join("access").join("objects")
    }

    fn object_path(&self, hash: &Digest) -> PathBuf {
        sharded_path(self.objects_dir(), hash)
    }

    fn thunk_cache_path(&self, key: &Digest) -> PathBuf {
        sharded_path(self.thunk_cache_dir(), key)
    }

    fn object_access_path(&self, hash: &Digest) -> PathBuf {
        sharded_path(self.access_dir(), hash)
    }

    fn temp_path(&self, path: &Path) -> PathBuf {
        let mut path = path.to_path_buf();
        let file_name = path
            .file_name()
            .expect("store path should have a file name")
            .to_string_lossy();
        let suffix = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        path.set_file_name(format!(
            ".{file_name}.{}.{}.tmp",
            std::process::id(),
            suffix
        ));
        path
    }

    fn ensure_parent_dir(&self, path: &Path) -> Result<(), StoreError> {
        let parent = path
            .parent()
            .expect("content-addressed object path should have a parent directory");
        fs::create_dir_all(parent).map_err(|source| {
            StoreError::io("create object directory", parent.to_path_buf(), source)
        })
    }

    pub fn gc(&self, roots: &[Ref]) -> Result<GcReport, StoreError> {
        let mut reachable = BTreeSet::new();
        for root in roots {
            self.mark_reachable(&root.hash, &mut reachable)?;
        }

        let mut report = GcReport {
            reachable: reachable.len(),
            ..GcReport::default()
        };

        for (hash, path) in self.object_entries()? {
            if reachable.contains(&hash) {
                report.kept_objects += 1;
            } else {
                fs::remove_file(&path)
                    .map_err(|source| StoreError::io("delete object", path.clone(), source))?;
                let _ = fs::remove_file(self.object_access_path(&hash));
                report.deleted_objects += 1;
            }
        }

        for (_, path) in self.thunk_cache_entries()? {
            let keep = match self.read_cached_thunk_at(&path)? {
                Some(CachedThunk::Value(reference) | CachedThunk::Lambda(reference)) => {
                    reachable.contains(&reference.hash)
                }
                Some(CachedThunk::Ref(reference)) => reachable.contains(&reference.hash),
                None => false,
            };
            if !keep {
                fs::remove_file(&path).map_err(|source| {
                    StoreError::io("delete thunk cache entry", path.clone(), source)
                })?;
                report.deleted_cache_entries += 1;
            }
        }

        Ok(report)
    }

    pub fn total_object_size(&self) -> Result<u64, StoreError> {
        self.object_entries()?
            .into_iter()
            .map(|(_, path)| {
                fs::metadata(&path)
                    .map(|metadata| metadata.len())
                    .map_err(|source| StoreError::io("stat object", path, source))
            })
            .try_fold(0_u64, |total, size| {
                size.map(|size| total.saturating_add(size))
            })
    }

    fn mark_reachable(
        &self,
        hash: &Digest,
        reachable: &mut BTreeSet<Digest>,
    ) -> Result<(), StoreError> {
        if !reachable.insert(*hash) {
            return Ok(());
        }

        let Some(stored) = self.get(&Ref::new(*hash))? else {
            return Ok(());
        };
        for reference in stored.references() {
            self.mark_reachable(&reference.hash, reachable)?;
        }
        Ok(())
    }

    fn enforce_max_size(&self, protected: Digest) -> Result<(), StoreError> {
        let Some(max_size) = self.max_size_bytes else {
            return Ok(());
        };
        let mut entries = self.object_entries_with_access()?;
        entries.sort_by_key(|entry| (entry.access_sequence, entry.hash));
        let mut total = entries
            .iter()
            .fold(0_u64, |total, entry| total.saturating_add(entry.size));

        for entry in entries {
            if total <= max_size {
                break;
            }
            if entry.hash == protected {
                continue;
            }
            fs::remove_file(&entry.path)
                .map_err(|source| StoreError::io("delete evicted object", entry.path, source))?;
            let _ = fs::remove_file(self.object_access_path(&entry.hash));
            total = total.saturating_sub(entry.size);
        }

        Ok(())
    }

    fn touch_object(&self, hash: &Digest) -> Result<(), StoreError> {
        let path = self.object_access_path(hash);
        self.ensure_parent_dir(&path)?;
        fs::write(&path, next_access_sequence().to_string())
            .map_err(|source| StoreError::io("write object access marker", path, source))
    }

    fn object_entries(&self) -> Result<Vec<(Digest, PathBuf)>, StoreError> {
        digest_entries(self.objects_dir())
    }

    fn thunk_cache_entries(&self) -> Result<Vec<(Digest, PathBuf)>, StoreError> {
        digest_entries(self.thunk_cache_dir())
    }

    fn object_entries_with_access(&self) -> Result<Vec<ObjectEntry>, StoreError> {
        self.object_entries()?
            .into_iter()
            .map(|(hash, path)| {
                let size = fs::metadata(&path)
                    .map(|metadata| metadata.len())
                    .map_err(|source| StoreError::io("stat object", path.clone(), source))?;
                let access_sequence = fs::read_to_string(self.object_access_path(&hash))
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                Ok(ObjectEntry {
                    hash,
                    path,
                    size,
                    access_sequence,
                })
            })
            .collect()
    }

    fn read_cached_thunk_at(&self, path: &Path) -> Result<Option<CachedThunk>, StoreError> {
        let encoded = match fs::read(path) {
            Ok(encoded) => encoded,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(StoreError::io(
                    "read thunk cache entry",
                    path.to_path_buf(),
                    source,
                ));
            }
        };

        serde_json::from_slice::<CachedThunk>(&encoded)
            .map(Some)
            .map_err(|source| StoreError::CacheDecode {
                path: path.to_path_buf(),
                source,
            })
    }
}

#[derive(Clone, Debug)]
struct ObjectEntry {
    hash: Digest,
    path: PathBuf,
    size: u64,
    access_sequence: u64,
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    objects: BTreeMap<Digest, Stored>,
    thunk_cache: BTreeMap<Digest, CachedThunk>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }
}

fn sharded_path(root: PathBuf, hash: &Digest) -> PathBuf {
    let rendered = hash.to_string();
    root.join(&rendered[..2]).join(&rendered[2..])
}

fn digest_entries(root: PathBuf) -> Result<Vec<(Digest, PathBuf)>, StoreError> {
    let mut entries = Vec::new();
    let directories = match fs::read_dir(&root) {
        Ok(directories) => directories,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(entries),
        Err(source) => return Err(StoreError::io("read store directory", root, source)),
    };

    for directory in directories {
        let directory = directory
            .map_err(|source| StoreError::io("read store directory", root.clone(), source))?;
        let directory_path = directory.path();
        if !directory_path.is_dir() {
            continue;
        }
        let files = fs::read_dir(&directory_path).map_err(|source| {
            StoreError::io(
                "read sharded store directory",
                directory_path.clone(),
                source,
            )
        })?;
        for file in files {
            let file = file.map_err(|source| {
                StoreError::io(
                    "read sharded store directory",
                    directory_path.clone(),
                    source,
                )
            })?;
            let path = file.path();
            if !path.is_file() {
                continue;
            }
            let Some(prefix) = directory_path.file_name().and_then(|name| name.to_str()) else {
                return Err(StoreError::InvalidObjectPath { path });
            };
            let Some(suffix) = path.file_name().and_then(|name| name.to_str()) else {
                return Err(StoreError::InvalidObjectPath { path });
            };
            let hash = Digest::from_str(&format!("{prefix}{suffix}"))
                .map_err(|_| StoreError::InvalidObjectPath { path: path.clone() })?;
            entries.push((hash, path));
        }
    }

    Ok(entries)
}

fn next_access_sequence() -> u64 {
    ACCESS_COUNTER.fetch_add(1, Ordering::Relaxed)
}

fn collect_term_refs(term: &Term, references: &mut Vec<Ref>) {
    match term {
        Term::Var(_) | Term::Value(_) => {}
        Term::Lambda(lambda) => collect_term_refs(&lambda.body, references),
        Term::Apply { callee, args } => {
            collect_term_refs(callee, references);
            for arg in args {
                collect_term_refs(arg, references);
            }
        }
        Term::Perform { args, .. } => {
            for arg in args {
                collect_term_refs(arg, references);
            }
        }
        Term::Handle { body, handlers } => {
            collect_term_refs(body, references);
            for handler in handlers.values() {
                collect_term_refs(handler, references);
            }
        }
        Term::Ref(reference) => references.push(reference.clone()),
        Term::Rec { bindings, body } => {
            for binding in bindings {
                collect_term_refs(&binding.lambda.body, references);
            }
            collect_term_refs(body, references);
        }
        Term::Case {
            scrutinee,
            branches,
        } => {
            collect_term_refs(scrutinee, references);
            for branch in branches {
                collect_term_refs(&branch.body, references);
            }
        }
        Term::Record(fields) => {
            for field in fields.values() {
                collect_term_refs(field, references);
            }
        }
        Term::List(items) => {
            for item in items {
                collect_term_refs(item, references);
            }
        }
    }
}

fn reference_for(object: &Stored) -> Result<Ref, StoreError> {
    if matches!(object, Stored::Term(term) if !term.is_closed()) {
        return Err(StoreError::OpenTerm);
    }

    Ok(Ref::new(object.digest()))
}

impl ObjectStore for FileStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError> {
        let reference = reference_for(&object)?;
        let path = self.object_path(&reference.hash);

        match fs::metadata(&path) {
            Ok(_) => {
                self.touch_object(&reference.hash)?;
                return Ok(reference);
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(StoreError::io("stat object", path, source)),
        }

        self.ensure_parent_dir(&path)?;
        let encoded = serde_json::to_vec(&object).map_err(StoreError::Encode)?;
        let temp_path = self.temp_path(&path);
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|source| StoreError::io("create temp object", temp_path.clone(), source))?;
        temp_file
            .write_all(&encoded)
            .map_err(|source| StoreError::io("write temp object", temp_path.clone(), source))?;
        temp_file
            .sync_all()
            .map_err(|source| StoreError::io("sync temp object", temp_path.clone(), source))?;

        match fs::rename(&temp_path, &path) {
            Ok(()) => {
                self.touch_object(&reference.hash)?;
                self.enforce_max_size(reference.hash)?;
                Ok(reference)
            }
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path);
                self.touch_object(&reference.hash)?;
                Ok(reference)
            }
            Err(source) => {
                let _ = fs::remove_file(&temp_path);
                Err(StoreError::io("install object", path, source))
            }
        }
    }

    fn get(&self, reference: &Ref) -> Result<Option<Stored>, StoreError> {
        let path = self.object_path(&reference.hash);
        let encoded = match fs::read(&path) {
            Ok(encoded) => encoded,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(StoreError::io("read object", path, source)),
        };
        let stored =
            serde_json::from_slice::<Stored>(&encoded).map_err(|source| StoreError::Decode {
                path: path.clone(),
                source,
            })?;
        let actual = stored.digest();
        if actual != reference.hash {
            return Err(StoreError::HashMismatch {
                path,
                expected: reference.hash,
                actual,
            });
        }

        self.touch_object(&reference.hash)?;
        Ok(Some(stored))
    }

    fn put_cached_thunk(&mut self, key: Digest, cached: CachedThunk) -> Result<(), StoreError> {
        let path = self.thunk_cache_path(&key);
        self.ensure_parent_dir(&path)?;

        let encoded = serde_json::to_vec(&cached).map_err(StoreError::Encode)?;
        let temp_path = self.temp_path(&path);
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(|source| {
                StoreError::io("create temp thunk cache entry", temp_path.clone(), source)
            })?;
        temp_file.write_all(&encoded).map_err(|source| {
            StoreError::io("write temp thunk cache entry", temp_path.clone(), source)
        })?;
        temp_file.sync_all().map_err(|source| {
            StoreError::io("sync temp thunk cache entry", temp_path.clone(), source)
        })?;

        match fs::rename(&temp_path, &path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path);
                Ok(())
            }
            Err(source) => {
                let _ = fs::remove_file(&temp_path);
                Err(StoreError::io("install thunk cache entry", path, source))
            }
        }
    }

    fn get_cached_thunk(&self, key: &Digest) -> Result<Option<CachedThunk>, StoreError> {
        let path = self.thunk_cache_path(key);
        let encoded = match fs::read(&path) {
            Ok(encoded) => encoded,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(StoreError::io("read thunk cache entry", path, source)),
        };

        serde_json::from_slice::<CachedThunk>(&encoded)
            .map(Some)
            .map_err(|source| StoreError::CacheDecode { path, source })
    }
}

impl ObjectStore for MemoryStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError> {
        let reference = reference_for(&object)?;
        let hash = reference.hash;
        self.objects.entry(hash).or_insert(object);
        Ok(reference)
    }

    fn get(&self, reference: &Ref) -> Result<Option<Stored>, StoreError> {
        Ok(self.objects.get(&reference.hash).cloned())
    }

    fn put_cached_thunk(&mut self, key: Digest, cached: CachedThunk) -> Result<(), StoreError> {
        self.thunk_cache.insert(key, cached);
        Ok(())
    }

    fn get_cached_thunk(&self, key: &Digest) -> Result<Option<CachedThunk>, StoreError> {
        Ok(self.thunk_cache.get(key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::Symbol;

    #[test]
    fn rejects_open_terms() {
        let mut store = MemoryStore::new();
        let open = Term::lambda(
            1,
            Term::Apply {
                callee: Box::new(Term::var(1)),
                args: vec![Term::var(0)],
            },
        );

        let result = store.put(Stored::term(open));
        assert!(matches!(result, Err(StoreError::OpenTerm)));
    }

    #[test]
    fn interns_and_loads_closed_objects() {
        let mut store = MemoryStore::new();
        let reference = store
            .put(Stored::value(Value::Symbol(Symbol::from("ok"))))
            .expect("value should store");

        let loaded = store.load(&reference).expect("value should exist");
        assert_eq!(loaded, Stored::value(Value::Symbol(Symbol::from("ok"))));
    }

    #[test]
    fn identical_objects_share_a_ref() {
        let mut store = MemoryStore::new();
        let left = store
            .put(Stored::value(Value::Integer(1)))
            .expect("value should store");
        let right = store
            .put(Stored::value(Value::Integer(1)))
            .expect("value should store");

        assert_eq!(left, right);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn file_store_persists_objects_across_reopen() {
        let root = unique_temp_dir("r2-file-store");
        let reference = {
            let mut store = FileStore::open(root.clone()).expect("store should open");
            store
                .put(Stored::value(Value::Symbol(Symbol::from("persisted"))))
                .expect("value should store")
        };

        let reopened = FileStore::open(root.clone()).expect("store should reopen");
        let loaded = reopened.load(&reference).expect("value should load");
        assert_eq!(
            loaded,
            Stored::value(Value::Symbol(Symbol::from("persisted")))
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_store_detects_corrupted_objects() {
        let root = unique_temp_dir("r2-file-store-corrupt");
        let mut store = FileStore::open(root.clone()).expect("store should open");
        let reference = store
            .put(Stored::value(Value::Integer(1)))
            .expect("value should store");
        let path = object_path(store.root(), &reference.hash);
        let corrupt = serde_json::to_vec(&Stored::value(Value::Integer(2)))
            .expect("corrupt object should encode");
        fs::write(&path, corrupt).expect("object should overwrite");

        let error = store
            .load(&reference)
            .expect_err("corruption should be detected");
        assert!(matches!(error, StoreError::HashMismatch { .. }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_store_gc_without_roots_deletes_objects() {
        let root = unique_temp_dir("r2-file-store-gc-empty");
        let mut store = FileStore::open(root.clone()).expect("store should open");
        let reference = store
            .put(Stored::value(Value::Integer(1)))
            .expect("value should store");

        let report = store.gc(&[]).expect("gc should run");

        assert_eq!(report.deleted_objects, 1);
        assert!(store.get(&reference).unwrap().is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_store_gc_keeps_transitively_reachable_objects() {
        let root = unique_temp_dir("r2-file-store-gc-rooted");
        let mut store = FileStore::open(root.clone()).expect("store should open");
        let child = store
            .put(Stored::value(Value::Symbol(Symbol::from("child"))))
            .expect("child should store");
        let root_ref = store
            .put(Stored::term(Term::Ref(child.clone())))
            .expect("root should store");
        let unreachable = store
            .put(Stored::value(Value::Symbol(Symbol::from("unreachable"))))
            .expect("unreachable should store");

        let report = store
            .gc(std::slice::from_ref(&root_ref))
            .expect("gc should run");

        assert_eq!(report.reachable, 2);
        assert_eq!(report.kept_objects, 2);
        assert_eq!(report.deleted_objects, 1);
        assert!(store.get(&root_ref).unwrap().is_some());
        assert!(store.get(&child).unwrap().is_some());
        assert!(store.get(&unreachable).unwrap().is_none());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_store_size_limit_evicts_lru_objects() {
        let root = unique_temp_dir("r2-file-store-max-size");
        let mut store = FileStore::open(root.clone())
            .expect("store should open")
            .with_max_size(120);

        for value in 0..20 {
            store
                .put(Stored::value(Value::Bytes(vec![value; 8])))
                .expect("value should store");
        }

        assert!(store.total_object_size().unwrap() <= 120);

        let _ = fs::remove_dir_all(root);
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();
        path.push(format!("{prefix}-{}-{nanos}", std::process::id()));
        path
    }

    fn object_path(root: &Path, hash: &Digest) -> PathBuf {
        let rendered = hash.to_string();
        root.join("objects")
            .join(&rendered[..2])
            .join(&rendered[2..])
    }
}
