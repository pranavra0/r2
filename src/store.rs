use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::data::{Canonical, Digest, Ref, Term, Value};

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    OpenTerm,
    MissingRef(Ref),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenTerm => f.write_str("store only accepts closed terms"),
            Self::MissingRef(reference) => write!(f, "missing store entry {}", reference.hash),
        }
    }
}

impl std::error::Error for StoreError {}

pub trait ObjectStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError>;
    fn get(&self, reference: &Ref) -> Option<Stored>;

    fn load(&self, reference: &Ref) -> Result<Stored, StoreError> {
        self.get(reference)
            .ok_or_else(|| StoreError::MissingRef(reference.clone()))
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

impl ObjectStore for MemoryStore {
    fn put(&mut self, object: Stored) -> Result<Ref, StoreError> {
        if matches!(&object, Stored::Term(term) if !term.is_closed()) {
            return Err(StoreError::OpenTerm);
        }

        let hash = object.digest();
        self.objects.entry(hash).or_insert(object);
        Ok(Ref::new(hash))
    }

    fn get(&self, reference: &Ref) -> Option<Stored> {
        self.objects.get(&reference.hash).cloned()
    }
}

#[cfg(test)]
mod tests {
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
}
