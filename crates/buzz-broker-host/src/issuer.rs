use std::io::Write as _;
use std::path::{Path, PathBuf};

use buzz_sdk::broker::AuthorityIdentity;

use crate::credential::{self, CredentialDigest};
use crate::store::{AuthorityStore, StoreError};

const MAX_AUTHORITY_FILE_BYTES: u64 = 64 * 1024;

pub(crate) async fn issue_to_file(
    store: &AuthorityStore,
    authority_file: &Path,
    credential_output: &Path,
) -> Result<(), IssuerError> {
    let identity = read_identity(authority_file)?;
    let credential = credential::generate();
    let digest = CredentialDigest::from_bearer(&credential)
        .map_err(|_| IssuerError::CredentialGeneration)?;
    persist_credential(credential_output, credential.as_bytes())?;
    if let Err(error) = store.issue(&digest, &identity).await {
        remove_unissued_credential(credential_output);
        return Err(IssuerError::Store(error));
    }
    Ok(())
}

pub(crate) async fn fence_from_file(
    store: &AuthorityStore,
    authority_file: &Path,
) -> Result<(), IssuerError> {
    let identity = read_identity(authority_file)?;
    store.fence(&identity).await.map_err(IssuerError::Store)
}

fn read_identity(path: &Path) -> Result<AuthorityIdentity, IssuerError> {
    let metadata = std::fs::metadata(path).map_err(|_| IssuerError::AuthorityFile)?;
    if !is_bounded_authority_file(&metadata) {
        return Err(IssuerError::AuthorityFile);
    }
    let bytes = std::fs::read(path).map_err(|_| IssuerError::AuthorityFile)?;
    let identity: AuthorityIdentity =
        serde_json::from_slice(&bytes).map_err(|_| IssuerError::AuthorityFile)?;
    identity.validated().map_err(|_| IssuerError::AuthorityFile)
}

fn is_bounded_authority_file(metadata: &std::fs::Metadata) -> bool {
    metadata.is_file() && metadata.len() <= MAX_AUTHORITY_FILE_BYTES
}

fn persist_credential(path: &Path, bytes: &[u8]) -> Result<(), IssuerError> {
    validate_output_path(path)?;
    let mut temporary = TemporaryCredential::create(path)?;
    temporary.write(bytes)?;
    temporary.persist(path)?;
    sync_parent(path)?;
    Ok(())
}

fn validate_output_path(path: &Path) -> Result<(), IssuerError> {
    if !path.is_absolute() {
        return Err(IssuerError::CredentialOutput);
    }
    let parent = path.parent().ok_or(IssuerError::CredentialOutput)?;
    if !std::fs::metadata(parent).is_ok_and(|metadata| metadata.is_dir()) {
        return Err(IssuerError::CredentialOutput);
    }
    if std::fs::symlink_metadata(path).is_ok() {
        return Err(IssuerError::CredentialOutputExists);
    }
    Ok(())
}

struct TemporaryCredential {
    path: Option<PathBuf>,
    file: std::fs::File,
}

impl TemporaryCredential {
    fn create(output: &Path) -> Result<Self, IssuerError> {
        let parent = output.parent().ok_or(IssuerError::CredentialOutput)?;
        let name = format!(".buzz-broker-credential-{}.tmp", uuid::Uuid::new_v4());
        let path = parent.join(name);
        let file = create_private_file(&path)?;
        Ok(Self {
            path: Some(path),
            file,
        })
    }

    fn write(&mut self, bytes: &[u8]) -> Result<(), IssuerError> {
        self.file
            .write_all(bytes)
            .and_then(|()| self.file.sync_all())
            .map_err(|_| IssuerError::CredentialOutput)
    }

    fn persist(&mut self, output: &Path) -> Result<(), IssuerError> {
        let temporary = self.path.as_ref().ok_or(IssuerError::CredentialOutput)?;
        std::fs::hard_link(temporary, output).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                IssuerError::CredentialOutputExists
            } else {
                IssuerError::CredentialOutput
            }
        })?;
        std::fs::remove_file(temporary).map_err(|_| IssuerError::CredentialOutput)?;
        self.path = None;
        Ok(())
    }
}

impl Drop for TemporaryCredential {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(unix)]
fn create_private_file(path: &Path) -> Result<std::fs::File, IssuerError> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| IssuerError::CredentialOutput)
}

#[cfg(not(unix))]
fn create_private_file(path: &Path) -> Result<std::fs::File, IssuerError> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_| IssuerError::CredentialOutput)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<(), IssuerError> {
    let parent = path.parent().ok_or(IssuerError::CredentialOutput)?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| IssuerError::CredentialOutput)
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<(), IssuerError> {
    Ok(())
}

fn remove_unissued_credential(path: &Path) {
    if std::fs::remove_file(path).is_ok() {
        let _ = sync_parent(path);
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum IssuerError {
    #[error("authority file is invalid")]
    AuthorityFile,
    #[error("credential generation failed")]
    CredentialGeneration,
    #[error("credential output path is invalid")]
    CredentialOutput,
    #[error("credential output already exists")]
    CredentialOutputExists,
    #[error(transparent)]
    Store(#[from] StoreError),
}
