use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hash(String);

impl Hash {
    pub fn new(domain: &str, bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(domain.as_bytes());
        hasher.update([0]);
        hasher.update(bytes);
        Self(format!("sha256:{}", hex::encode(hasher.finalize())))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn short(&self) -> &str {
        self.0
            .strip_prefix("sha256:")
            .and_then(|hex| hex.get(..12))
            .unwrap_or(&self.0)
    }

    pub fn shard(&self) -> &str {
        self.0
            .strip_prefix("sha256:")
            .and_then(|hex| hex.get(..2))
            .unwrap_or("xx")
    }

    pub fn body(&self) -> &str {
        self.0.strip_prefix("sha256:").unwrap_or(&self.0)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Hash {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with("sha256:") {
            anyhow::bail!("hash must start with sha256:");
        }

        let body = &s["sha256:".len()..];
        if body.len() != 64 || !body.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            anyhow::bail!("hash must contain 64 hex characters");
        }

        Ok(Self(s.to_owned()))
    }
}
