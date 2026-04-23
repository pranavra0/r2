use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::data::{Canonical, Digest, Ref, Term, Value};

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Stored {
    Term(Term),
    Value(Value),
}

impl Stored {
    pub fn term(term: Term) -> Self {
        Self::Term(term)
    }

    pub fn value(value: Value) -> Self {
        Self::Value(value)
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
    HashMismatch {
        path: PathBuf,
        expected: Digest,
        actual: Digest,
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
            Self::HashMismatch {
                path,
                expected,
                actual,
            } => write!(
                f,
                "store object {} had digest {actual}, expected {expected}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Encode(source) => Some(source),
            Self::Decode { source, .. } => Some(source),
            Self::OpenTerm | Self::MissingRef(_) | Self::HashMismatch { .. } => None,
        }
    }
}

pub trait ObjectStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError>;
    fn get(&self, reference: &Ref) -> Result<Option<Stored>, StoreError>;

    fn load(&self, reference: &Ref) -> Result<Stored, StoreError> {
        self.get(reference)
            .and_then(|stored| stored.ok_or_else(|| StoreError::MissingRef(reference.clone())))
    }
}

#[derive(Clone, Debug)]
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let store = Self { root: root.into() };
        let objects_dir = store.objects_dir();
        fs::create_dir_all(&objects_dir)
            .map_err(|source| StoreError::io("create store directory", objects_dir, source))?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn objects_dir(&self) -> PathBuf {
        self.root.join("objects")
    }

    fn object_path(&self, hash: &Digest) -> PathBuf {
        let rendered = hash.to_string();
        self.objects_dir().join(&rendered[..2]).join(&rendered[2..])
    }

    fn temp_path(&self, hash: &Digest) -> PathBuf {
        let mut path = self.object_path(hash);
        let file_name = path
            .file_name()
            .expect("content-addressed object path should have a file name")
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
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    objects: BTreeMap<Digest, Stored>,
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
            Ok(_) => return Ok(reference),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => return Err(StoreError::io("stat object", path, source)),
        }

        self.ensure_parent_dir(&path)?;
        let encoded = serde_json::to_vec(&object).map_err(StoreError::Encode)?;
        let temp_path = self.temp_path(&reference.hash);
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
            Ok(()) => Ok(reference),
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path);
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

        Ok(Some(stored))
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
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::data::Symbol;

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
