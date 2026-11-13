use base64::Engine as _;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

const PREFIX: &str = "bzh_";
const TOKEN_BYTES: usize = 32;
const ENCODED_BYTES: usize = 43;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CredentialDigest([u8; 32]);

impl CredentialDigest {
    pub(crate) fn from_bearer(value: &str) -> Result<Self, CredentialError> {
        let encoded = value
            .strip_prefix(PREFIX)
            .ok_or(CredentialError::Malformed)?;
        if encoded.len() != ENCODED_BYTES {
            return Err(CredentialError::Malformed);
        }
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| CredentialError::Malformed)?;
        if decoded.len() != TOKEN_BYTES {
            return Err(CredentialError::Malformed);
        }
        Ok(Self(Sha256::digest(value.as_bytes()).into()))
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

pub(crate) fn generate() -> Zeroizing<String> {
    let random: [u8; TOKEN_BYTES] = rand::random();
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(random);
    Zeroizing::new(format!("{PREFIX}{encoded}"))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CredentialError {
    #[error("broker credential is malformed")]
    Malformed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_credentials_are_canonical_and_distinct() {
        let first = generate();
        let second = generate();
        assert_ne!(first.as_str(), second.as_str());
        assert_eq!(first.len(), PREFIX.len() + ENCODED_BYTES);
        assert!(CredentialDigest::from_bearer(&first).is_ok());
    }

    #[test]
    fn malformed_credentials_fail_closed() {
        for value in [
            "",
            "Bearer token",
            "bzh_short",
            "bzh_!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",
        ] {
            assert!(CredentialDigest::from_bearer(value).is_err());
        }
    }
}
