use super::*;

use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt as _;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};

// A pending retry can query, publish, then query again (3 x 20s HTTP timeout).
const MAX_LOCK_WAIT: Duration = Duration::from_secs(75);

pub(crate) struct PublicationGuard {
    _lock: Flock<File>,
}

impl AuthorityStore {
    pub(crate) async fn publication_lock_for_credential(
        &self,
        credential: &CredentialDigest,
    ) -> Result<PublicationGuard, StoreError> {
        self.publication_lock_for_hash(credential.as_bytes().to_vec())
            .await
    }

    pub(crate) async fn publication_lock_for_identity(
        &self,
        identity: &AuthorityIdentity,
    ) -> Result<PublicationGuard, StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        let hash: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT credential_hash FROM broker_authorities WHERE authority_json = ?",
        )
        .bind(authority_json)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        let Some(hash) = hash else {
            self.validate_authority_state().await?;
            return Err(StoreError::AuthorityNotFound);
        };
        self.publication_lock_for_hash(hash).await
    }

    async fn publication_lock_for_hash(
        &self,
        hash: Vec<u8>,
    ) -> Result<PublicationGuard, StoreError> {
        if hash.len() != 32 {
            return Err(StoreError::Integrity);
        }
        let directory = self.state_dir.clone();
        let path = directory.join(format!(".effect-{}.lock", hex::encode(hash)));
        tokio::task::spawn_blocking(move || {
            let metadata = std::fs::metadata(&directory).map_err(|_| StoreError::Unavailable)?;
            require_private_directory(&metadata)?;
            let mut file = match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let entry =
                        std::fs::symlink_metadata(&path).map_err(|_| StoreError::Unavailable)?;
                    if entry.file_type().is_symlink() {
                        return Err(StoreError::InsecureFile);
                    }
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&path)
                        .map_err(|_| StoreError::Unavailable)?
                }
                Err(_) => return Err(StoreError::Unavailable),
            };
            let metadata = file.metadata().map_err(|_| StoreError::Unavailable)?;
            if !is_private_regular_file(&metadata) {
                return Err(StoreError::InsecureFile);
            }
            let deadline = Instant::now() + MAX_LOCK_WAIT;
            loop {
                match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
                    Ok(lock) => return Ok(PublicationGuard { _lock: lock }),
                    Err((returned, Errno::EWOULDBLOCK)) if Instant::now() < deadline => {
                        file = returned;
                        std::thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => return Err(StoreError::Unavailable),
                }
            }
        })
        .await
        .map_err(|_| StoreError::Unavailable)?
    }
}
