use crate::{Hash, Node, Outcome, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
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
        self.put_object("node", node)
    }

    pub fn get_node(&self, hash: &Hash) -> anyhow::Result<Option<Node>> {
        self.get_object(hash)
    }

    pub fn put_value(&self, value: &Value) -> anyhow::Result<Hash> {
        self.put_object("value", value)
    }

    pub fn get_value(&self, hash: &Hash) -> anyhow::Result<Option<Value>> {
        self.get_object(hash)
    }

    pub fn put_outcome(&self, node: &Hash, outcome: &Outcome) -> anyhow::Result<()> {
        self.put_at(&self.outcome_path(node), outcome)
    }

    pub fn get_outcome(&self, node: &Hash) -> anyhow::Result<Option<Outcome>> {
        self.get_at(&self.outcome_path(node))
    }

    fn put_object<T: Serialize>(&self, domain: &str, object: &T) -> anyhow::Result<Hash> {
        let bytes = encode(object)?;
        let hash = Hash::new(domain, &bytes);
        self.put_bytes(&self.object_path(&hash), &bytes)?;
        Ok(hash)
    }

    fn get_object<T: DeserializeOwned>(&self, hash: &Hash) -> anyhow::Result<Option<T>> {
        self.get_at(&self.object_path(hash))
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
        Ok(Some(serde_json::from_slice(&bytes)?))
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
            .join(format!("{}.json", hash.body()))
    }

    fn outcome_path(&self, hash: &Hash) -> PathBuf {
        self.root
            .join("outcomes")
            .join(hash.shard())
            .join(format!("{}.json", hash.body()))
    }
}

fn encode<T: Serialize>(object: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(object)?)
}
