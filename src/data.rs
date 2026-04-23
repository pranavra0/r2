use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const CANONICAL_MAGIC: &[u8; 4] = b"r2v1";

pub trait Canonical {
    fn write_canonical(&self, out: &mut Vec<u8>);

    fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(CANONICAL_MAGIC);
        self.write_canonical(&mut out);
        out
    }

    fn digest(&self) -> Digest {
        Digest::blake3(self.canonical_bytes())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Symbol(String);

impl Symbol {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Symbol {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Canonical for Symbol {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        write_string(out, self.as_str());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VarIndex(u32);

impl VarIndex {
    pub fn new(index: u32) -> Self {
        Self(index)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

impl Canonical for VarIndex {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        write_u32(out, self.get());
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Digest([u8; 32]);

impl Digest {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn blake3(bytes: impl AsRef<[u8]>) -> Self {
        Self(blake3::hash(bytes.as_ref()).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Digest {
    type Err = ParseDigestError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != 64 {
            return Err(ParseDigestError::WrongLength { found: input.len() });
        }

        let mut bytes = [0_u8; 32];

        for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
            let high = decode_hex(chunk[0]).ok_or(ParseDigestError::InvalidHex {
                index: index * 2,
                byte: chunk[0],
            })?;
            let low = decode_hex(chunk[1]).ok_or(ParseDigestError::InvalidHex {
                index: index * 2 + 1,
                byte: chunk[1],
            })?;
            bytes[index] = (high << 4) | low;
        }

        Ok(Self::new(bytes))
    }
}

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DigestVisitor;

        impl<'de> Visitor<'de> for DigestVisitor {
            type Value = Digest;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a 64-character lowercase hex digest")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Digest::from_str(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DigestVisitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseDigestError {
    WrongLength { found: usize },
    InvalidHex { index: usize, byte: u8 },
}

impl fmt::Display for ParseDigestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { found } => {
                write!(f, "expected 64 hex characters, found {found}")
            }
            Self::InvalidHex { index, byte } => {
                write!(f, "invalid hex byte 0x{byte:02x} at position {index}")
            }
        }
    }
}

impl std::error::Error for ParseDigestError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ref {
    pub hash: Digest,
}

impl Ref {
    pub fn new(hash: Digest) -> Self {
        Self { hash }
    }
}

impl Canonical for Ref {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.hash.as_bytes());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Value {
    Integer(i64),
    Symbol(Symbol),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Record(BTreeMap<Symbol, Value>),
    Tagged { tag: Symbol, fields: Vec<Value> },
}

impl Canonical for Value {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        match self {
            Self::Integer(value) => {
                out.push(0x01);
                write_i64(out, *value);
            }
            Self::Symbol(symbol) => {
                out.push(0x02);
                symbol.write_canonical(out);
            }
            Self::Bytes(bytes) => {
                out.push(0x03);
                write_bytes(out, bytes);
            }
            Self::List(items) => {
                out.push(0x04);
                write_len(out, items.len());
                for item in items {
                    item.write_canonical(out);
                }
            }
            Self::Record(entries) => {
                out.push(0x05);
                write_len(out, entries.len());
                for (key, value) in entries {
                    key.write_canonical(out);
                    value.write_canonical(out);
                }
            }
            Self::Tagged { tag, fields } => {
                out.push(0x06);
                tag.write_canonical(out);
                write_len(out, fields.len());
                for field in fields {
                    field.write_canonical(out);
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lambda {
    pub parameters: u16,
    pub body: Box<Term>,
}

impl Lambda {
    pub fn new(parameters: u16, body: Term) -> Self {
        Self {
            parameters,
            body: Box::new(body),
        }
    }
}

impl Canonical for Lambda {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        write_u16(out, self.parameters);
        self.body.write_canonical(out);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum Term {
    // Variables are binding machinery for the IR, not a semantic primitive.
    Var(VarIndex),
    Value(Value),
    Lambda(Lambda),
    Apply {
        callee: Box<Term>,
        args: Vec<Term>,
    },
    Perform {
        op: Symbol,
        args: Vec<Term>,
    },
    Handle {
        body: Box<Term>,
        handlers: BTreeMap<Symbol, Term>,
    },
    Ref(Ref),
}

impl Term {
    pub fn var(index: u32) -> Self {
        Self::Var(VarIndex::new(index))
    }

    pub fn lambda(parameters: u16, body: Term) -> Self {
        Self::Lambda(Lambda::new(parameters, body))
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed_at(0)
    }

    fn is_closed_at(&self, depth: u32) -> bool {
        match self {
            Self::Var(index) => index.get() < depth,
            Self::Value(_) => true,
            Self::Lambda(lambda) => lambda
                .body
                .is_closed_at(depth.saturating_add(u32::from(lambda.parameters))),
            Self::Apply { callee, args } => {
                callee.is_closed_at(depth) && args.iter().all(|arg| arg.is_closed_at(depth))
            }
            Self::Perform { op: _, args } => args.iter().all(|arg| arg.is_closed_at(depth)),
            Self::Handle { body, handlers } => {
                body.is_closed_at(depth)
                    && handlers.values().all(|handler| handler.is_closed_at(depth))
            }
            Self::Ref(_) => true,
        }
    }
}

impl Canonical for Term {
    fn write_canonical(&self, out: &mut Vec<u8>) {
        match self {
            Self::Var(index) => {
                out.push(0x10);
                index.write_canonical(out);
            }
            Self::Value(value) => {
                out.push(0x11);
                value.write_canonical(out);
            }
            Self::Lambda(lambda) => {
                out.push(0x12);
                lambda.write_canonical(out);
            }
            Self::Apply { callee, args } => {
                out.push(0x13);
                callee.write_canonical(out);
                write_len(out, args.len());
                for arg in args {
                    arg.write_canonical(out);
                }
            }
            Self::Perform { op, args } => {
                out.push(0x14);
                op.write_canonical(out);
                write_len(out, args.len());
                for arg in args {
                    arg.write_canonical(out);
                }
            }
            Self::Handle { body, handlers } => {
                out.push(0x15);
                body.write_canonical(out);
                write_len(out, handlers.len());
                for (op, handler) in handlers {
                    op.write_canonical(out);
                    handler.write_canonical(out);
                }
            }
            Self::Ref(reference) => {
                out.push(0x16);
                reference.write_canonical(out);
            }
        }
    }
}

fn write_len(out: &mut Vec<u8>, len: usize) {
    let len = u64::try_from(len).expect("canonical length exceeds u64");
    out.extend_from_slice(&len.to_be_bytes());
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_string(out: &mut Vec<u8>, value: &str) {
    write_bytes(out, value.as_bytes());
}

fn write_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    write_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn decode_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn demo_term() -> Term {
        let mut handlers = BTreeMap::new();
        handlers.insert(
            Symbol::from("fs.read"),
            Term::lambda(
                2,
                Term::Apply {
                    callee: Box::new(Term::var(1)),
                    args: vec![Term::Value(Value::Symbol(Symbol::from("handled")))],
                },
            ),
        );

        let mut record = BTreeMap::new();
        record.insert(Symbol::from("name"), Value::Symbol(Symbol::from("hello")));
        record.insert(Symbol::from("version"), Value::Integer(1));

        Term::Handle {
            body: Box::new(Term::Perform {
                op: Symbol::from("fs.read"),
                args: vec![Term::Value(Value::Record(record))],
            }),
            handlers,
        }
    }

    #[test]
    fn serde_roundtrip_preserves_terms() {
        let term = demo_term();
        let json = serde_json::to_string_pretty(&term).expect("term should serialize");
        let decoded: Term = serde_json::from_str(&json).expect("term should deserialize");

        assert_eq!(decoded, term);
    }

    #[test]
    fn digest_is_stable_across_map_insertion_order() {
        let mut left_handlers = BTreeMap::new();
        left_handlers.insert(Symbol::from("b"), Term::Value(Value::Integer(2)));
        left_handlers.insert(Symbol::from("a"), Term::Value(Value::Integer(1)));

        let mut right_handlers = BTreeMap::new();
        right_handlers.insert(Symbol::from("a"), Term::Value(Value::Integer(1)));
        right_handlers.insert(Symbol::from("b"), Term::Value(Value::Integer(2)));

        let left = Term::Handle {
            body: Box::new(Term::Value(Value::Symbol(Symbol::from("ok")))),
            handlers: left_handlers,
        };
        let right = Term::Handle {
            body: Box::new(Term::Value(Value::Symbol(Symbol::from("ok")))),
            handlers: right_handlers,
        };

        assert_eq!(left.canonical_bytes(), right.canonical_bytes());
        assert_eq!(left.digest(), right.digest());
    }

    #[test]
    fn structurally_distinct_terms_hash_differently() {
        let left = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Value(Value::Integer(1))],
        };
        let right = Term::Apply {
            callee: Box::new(Term::lambda(1, Term::var(0))),
            args: vec![Term::Value(Value::Integer(2))],
        };

        assert_ne!(left.digest(), right.digest());
    }

    #[test]
    fn digest_display_and_parse_roundtrip() {
        let digest = demo_term().digest();
        let rendered = digest.to_string();
        let parsed = Digest::from_str(&rendered).expect("digest should parse");

        assert_eq!(parsed, digest);
    }

    #[test]
    fn closedness_tracks_bound_variables() {
        let open = Term::lambda(
            1,
            Term::Apply {
                callee: Box::new(Term::var(1)),
                args: vec![Term::var(0)],
            },
        );
        let closed = Term::lambda(
            1,
            Term::lambda(
                1,
                Term::Apply {
                    callee: Box::new(Term::var(1)),
                    args: vec![Term::var(0)],
                },
            ),
        );

        assert!(!open.is_closed());
        assert!(closed.is_closed());
    }
}
