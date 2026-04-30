use serde::Serialize;
use serde::de::DeserializeOwned;

pub fn to_canonical_vec<T: Serialize>(value: &T) -> anyhow::Result<Vec<u8>> {
    Ok(postcard::to_allocvec(value)?)
}

pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(postcard::from_bytes(bytes)?)
}
