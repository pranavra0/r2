use crate::Hash;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphTrace {
    pub dependency_path: Vec<Hash>,
}

impl GraphTrace {
    pub fn new(dependency_path: Vec<Hash>) -> Self {
        Self { dependency_path }
    }

    pub fn push(&mut self, hash: Hash) {
        self.dependency_path.push(hash);
    }

    pub fn pop(&mut self) {
        self.dependency_path.pop();
    }

    pub fn contains(&self, hash: &Hash) -> bool {
        self.dependency_path.contains(hash)
    }

    pub fn hashes(&self) -> &[Hash] {
        &self.dependency_path
    }
}

impl fmt::Display for GraphTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, hash) in self.dependency_path.iter().enumerate() {
            if index == 0 {
                writeln!(f, "{}", hash.short())?;
            } else {
                writeln!(f, "  -> {}", hash.short())?;
            }
        }

        Ok(())
    }
}
