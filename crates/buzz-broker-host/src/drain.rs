use buzz_sdk::broker::AuthorityIdentity;

use crate::publisher::{PublishError, RelayPublisher};
use crate::signing::HostSigner;
use crate::store::{AuthorityStore, StoreError};

pub(crate) async fn drain_pending(
    store: &AuthorityStore,
    identity: &AuthorityIdentity,
    signer: &HostSigner,
    publisher: &RelayPublisher,
) -> Result<bool, DrainError> {
    let _publication_guard = store.publication_lock_for_identity(identity).await?;
    let Some(pending) = store.pending_message_post(identity).await? else {
        return Ok(false);
    };
    if !signer.matches_executor(identity) || !signer.matches_event_owner(&pending.event) {
        return Err(StoreError::InvalidIdentity.into());
    }
    publisher
        .publish_pending(signer, &pending.relay_url, &pending.event)
        .await?;
    store
        .complete_message_post(
            &pending.credential,
            &pending.request_id,
            &pending.event.id.to_hex(),
        )
        .await?;
    Ok(true)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum DrainError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Publish(#[from] PublishError),
}
