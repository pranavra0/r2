use crate::encode;
use crate::{Hash, Node, Outcome, Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Store {
    root: PathBuf,
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

    pub fn put_outcome(&self, node: &Hash, outcome: &Outcome) -> anyhow::Result<()> {
        self.put_at(&self.outcome_path(node), outcome)
    }

    pub fn get_outcome(&self, node: &Hash) -> anyhow::Result<Option<Outcome>> {
        self.get_at(&self.outcome_path(node))
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

    fn put_at<T: Serialize>(&self, path: &Path, object: &T) -> anyhow::Result<()> {
        let bytes = encode(object)?;
        self.put_bytes(path, &bytes)
    }

    fn get_at<T: DeserializeOwned>(&self, path: &Path) -> anyhow::Result<Option<T>> {
        if !path.exists() {
            return Ok(None);
        }

        let bytes = fs::read(path)?;
        Ok(Some(encode::from_slice(&bytes)?))
    }

    fn put_bytes(&self, path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
        if path.exists() {
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let tmp = path.with_extension("tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(tmp, path)?;
        Ok(())
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
