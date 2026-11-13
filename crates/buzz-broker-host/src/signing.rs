use std::path::Path;

use buzz_sdk::broker::{AuthorityIdentity, MessagePostArgs, PubkeyHex, StorageAddress};
use nostr::{Event, EventBuilder, JsonUtil as _, Keys, Kind, PublicKey, Tag};
use sha2::{Digest as _, Sha256};
use zeroize::Zeroizing;

pub(crate) struct HostSigner {
    keys: Keys,
    owner_pubkey: PublicKey,
    auth_tag: Tag,
    auth_tag_json: String,
}

impl HostSigner {
    pub(crate) fn load(key_path: &Path, tag_path: &Path) -> Result<Self, SigningError> {
        require_private_file(key_path)?;
        require_private_file(tag_path)?;
        let key = Zeroizing::new(
            std::fs::read_to_string(key_path).map_err(|_| SigningError::Unreadable)?,
        );
        let keys = Keys::parse(key.trim()).map_err(|_| SigningError::InvalidKey)?;
        let tag = std::fs::read_to_string(tag_path).map_err(|_| SigningError::Unreadable)?;
        let owner_pubkey = buzz_sdk::nip_oa::verify_auth_tag(tag.trim(), &keys.public_key())
            .map_err(|_| SigningError::InvalidAttestation)?;
        let auth_tag = buzz_sdk::nip_oa::parse_auth_tag(tag.trim())
            .map_err(|_| SigningError::InvalidAttestation)?;
        Ok(Self {
            keys,
            owner_pubkey,
            auth_tag,
            auth_tag_json: tag.trim().to_owned(),
        })
    }

    pub(crate) fn auth_tag_json(&self) -> &str {
        &self.auth_tag_json
    }

    pub(crate) fn pubkey_hex(&self) -> String {
        self.keys.public_key().to_hex()
    }

    pub(crate) fn owner_hex(&self) -> String {
        self.owner_pubkey.to_hex()
    }

    pub(crate) fn cursor_mac(&self, payload: &[u8]) -> [u8; 16] {
        let key = Zeroizing::new(self.keys.secret_key().to_secret_hex());
        let mut inner_pad = Zeroizing::new([0x36_u8; 64]);
        let mut outer_pad = Zeroizing::new([0x5c_u8; 64]);
        for (index, byte) in key.as_bytes().iter().enumerate() {
            inner_pad[index] ^= byte;
            outer_pad[index] ^= byte;
        }
        let mut inner = Sha256::new();
        inner.update(*inner_pad);
        inner.update(payload);
        let mut outer = Sha256::new();
        outer.update(*outer_pad);
        outer.update(inner.finalize());
        let digest = outer.finalize();
        let mut mac = [0_u8; 16];
        mac.copy_from_slice(&digest[..16]);
        mac
    }

    pub(crate) fn matches_executor(&self, identity: &AuthorityIdentity) -> bool {
        identity.executor_agent_pubkey.as_str() == self.keys.public_key().to_hex()
    }

    pub(crate) fn matches_event_owner(&self, event: &Event) -> bool {
        let mut owners = event.tags.iter().filter_map(|tag| {
            let encoded = serde_json::to_string(tag).ok()?;
            buzz_sdk::nip_oa::verify_auth_tag(&encoded, &event.pubkey).ok()
        });
        owners
            .next()
            .is_some_and(|owner| owner == self.owner_pubkey)
            && owners.next().is_none()
    }

    pub(crate) fn storage_address(&self, slug: &str) -> Result<StorageAddress, SigningError> {
        let author_pubkey = PubkeyHex::parse(self.keys.public_key().to_hex())
            .map_err(|_| SigningError::InvalidKey)?;
        let key = buzz_core::engram::conversation_key(self.keys.secret_key(), &self.owner_pubkey);
        Ok(StorageAddress {
            author_pubkey,
            kind: buzz_core::kind::KIND_AGENT_ENGRAM,
            d_tag: buzz_core::engram::d_tag(&key, slug),
        })
    }

    pub(crate) fn nip98_authorization(
        &self,
        url: &str,
        body: &[u8],
    ) -> Result<String, SigningError> {
        use base64::Engine as _;

        let payload = hex::encode(Sha256::digest(body));
        let tags = [
            Tag::parse(["u", url]).map_err(|_| SigningError::Sign)?,
            Tag::parse(["method", "POST"]).map_err(|_| SigningError::Sign)?,
            Tag::parse(["nonce", &uuid::Uuid::new_v4().to_string()])
                .map_err(|_| SigningError::Sign)?,
            Tag::parse(["payload", &payload]).map_err(|_| SigningError::Sign)?,
        ];
        let auth = EventBuilder::new(Kind::Custom(27235), "")
            .tags(tags)
            .sign_with_keys(&self.keys)
            .map_err(|_| SigningError::Sign)?;
        Ok(format!(
            "Nostr {}",
            base64::engine::general_purpose::STANDARD.encode(auth.as_json().as_bytes())
        ))
    }

    pub(crate) fn sign_message(
        &self,
        identity: &AuthorityIdentity,
        args: &MessagePostArgs,
    ) -> Result<Event, SigningError> {
        if !self.matches_executor(identity) {
            return Err(SigningError::ExecutorMismatch);
        }
        let args = args.validated().map_err(|_| SigningError::InvalidMessage)?;
        let channel =
            uuid::Uuid::parse_str(&args.channel_id).map_err(|_| SigningError::InvalidMessage)?;
        let mentions: Vec<&str> = args.mentions.iter().map(PubkeyHex::as_str).collect();
        buzz_sdk::build_message(channel, &args.content, None, &mentions, false, &[])
            .map_err(|_| SigningError::InvalidMessage)?
            .tags([self.auth_tag.clone()])
            .sign_with_keys(&self.keys)
            .map_err(|_| SigningError::Sign)
    }
}

fn require_private_file(path: &Path) -> Result<(), SigningError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    if !path.is_absolute() {
        return Err(SigningError::InsecureFile);
    }
    let parent = path.parent().ok_or(SigningError::InsecureFile)?;
    let directory = std::fs::symlink_metadata(parent).map_err(|_| SigningError::Unreadable)?;
    if !directory.is_dir()
        || directory.file_type().is_symlink()
        || directory.uid() != nix::unistd::geteuid().as_raw()
        || directory.permissions().mode() & 0o077 != 0
    {
        return Err(SigningError::InsecureFile);
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| SigningError::Unreadable)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(SigningError::InsecureFile);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SigningError {
    #[error("signer file must be private and owned by the broker user")]
    InsecureFile,
    #[error("signer file is unavailable")]
    Unreadable,
    #[error("signer key is invalid")]
    InvalidKey,
    #[error("owner attestation is invalid for the signer key")]
    InvalidAttestation,
    #[error("signer key does not match the authority executor")]
    ExecutorMismatch,
    #[error("message arguments are invalid")]
    InvalidMessage,
    #[error("event signing failed")]
    Sign,
}
