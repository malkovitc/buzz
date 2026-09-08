//! Managed-ACP generation admission through the authenticated broker session.
//!
//! The runtime supplies an expected, non-secret identity only so the harness
//! can detect mis-provisioning. Authority itself comes from the broker's
//! `authority.status` verdict and from the broker revalidating the same bearer
//! before every reply or effect action.

use crate::HttpBrokerClient;
use buzz_sdk::broker::{
    ActionArgs, ActionOutcome, AuthorityIdentity, AuthorityState, AuthorityStatusArgs,
    BrokerClientExt, BrokerRequest, BrokerResult, ManagedAcpAuthority, RuntimeSupport,
};
use thiserror::Error;
use uuid::Uuid;

/// A validated expected identity paired with one authenticated broker client.
///
/// Clones share reqwest's connection pool. The bearer credential remains
/// private inside [`HttpBrokerClient`] and is never exposed by this API.
#[derive(Clone)]
pub struct AuthorityFence {
    client: HttpBrokerClient,
    expected: AuthorityIdentity,
}

/// Fail-closed managed-ACP admission errors. Messages deliberately omit
/// authority values because task and destination identifiers are private
/// operator metadata.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthorityError {
    /// Expected launch metadata was malformed.
    #[error("managed ACP authority expectation is invalid")]
    InvalidExpectation,
    /// No correlated host verdict was obtained.
    #[error("managed ACP authority host is unavailable")]
    HostUnavailable,
    /// The host returned a verdict other than success.
    #[error("managed ACP authority was rejected by the host")]
    Rejected,
    /// A success carried a different action outcome.
    #[error("managed ACP authority host returned the wrong outcome")]
    WrongOutcome,
    /// The credential is bound to another normalized identity.
    #[error("managed ACP authority binding does not match this launch")]
    BindingMismatch,
    /// This generation is quiescing or terminally fenced.
    #[error("managed ACP generation is not active")]
    NotActive,
    /// The selected runtime cannot safely participate in handoff.
    #[error("managed ACP runtime is unsupported for handoff")]
    UnsupportedRuntime,
}

impl AuthorityFence {
    /// Build a fence from validated launch metadata and broker provisioning.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorityError::InvalidExpectation`] if any identity member is
    /// malformed. Broker URL and credential validation errors are intentionally
    /// mapped to the same value-safe startup failure.
    pub fn new(
        base_url: String,
        credential: String,
        expected: AuthorityIdentity,
    ) -> Result<Self, AuthorityError> {
        let expected = expected
            .validated()
            .map_err(|_| AuthorityError::InvalidExpectation)?;
        let client = HttpBrokerClient::new(base_url, credential)
            .map_err(|_| AuthorityError::InvalidExpectation)?;
        Ok(Self { client, expected })
    }

    /// Require a current, exact and runnable host verdict.
    ///
    /// This is called immediately before every ACP process spawn and mutating
    /// prompt. [`Self::execute`] applies the same check to broker effects.
    ///
    /// # Errors
    ///
    /// Fails closed for transport errors, host rejection, stale lifecycle
    /// state, unsupported runtimes, or any identity mismatch.
    pub async fn permit(&self) -> Result<(), AuthorityError> {
        let authority = self.status().await?;
        if authority.state != AuthorityState::Active {
            return Err(AuthorityError::NotActive);
        }
        if authority.identity.runtime_support == RuntimeSupport::Unsupported {
            return Err(AuthorityError::UnsupportedRuntime);
        }
        if authority.identity != self.expected {
            return Err(AuthorityError::BindingMismatch);
        }
        Ok(())
    }

    /// Execute one broker action only after a fresh generation permit.
    ///
    /// The host must additionally make generation validation and effect commit
    /// atomic; this preflight prevents supported clients from issuing an action
    /// after an observed cutover.
    ///
    /// # Errors
    ///
    /// Returns a value-safe authority error before the requested action on any
    /// stale, mismatched, rejected, or unavailable verdict.
    pub async fn execute(&self, args: ActionArgs) -> Result<ActionOutcome, AuthorityError> {
        self.permit().await?;
        self.execute_unfenced(args).await
    }

    async fn status(&self) -> Result<ManagedAcpAuthority, AuthorityError> {
        match self
            .execute_unfenced(ActionArgs::AuthorityStatus(AuthorityStatusArgs {}))
            .await?
        {
            ActionOutcome::AuthorityStatus(authority) => Ok(authority),
            _ => Err(AuthorityError::WrongOutcome),
        }
    }

    async fn execute_unfenced(&self, args: ActionArgs) -> Result<ActionOutcome, AuthorityError> {
        let request = BrokerRequest::new(Uuid::new_v4().to_string(), args)
            .and_then(BrokerRequest::prepare)
            .map_err(|_| AuthorityError::InvalidExpectation)?;
        let response = self
            .client
            .execute(&request)
            .await
            .map_err(|_| AuthorityError::HostUnavailable)?;
        match response.into_envelope().result {
            BrokerResult::Succeeded { outcome } => Ok(outcome),
            BrokerResult::Failed { .. } | BrokerResult::Indeterminate { .. } => {
                Err(AuthorityError::Rejected)
            }
        }
    }

    /// Expected signing identity used to correlate `storage.address`.
    #[must_use]
    pub fn expected_executor_pubkey(&self) -> &buzz_sdk::broker::PubkeyHex {
        &self.expected.executor_agent_pubkey
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, Bytes};
    use axum::http::StatusCode;
    use axum::response::Response;
    use axum::routing::post;
    use axum::Router;
    use buzz_sdk::broker::{PubkeyHex, RuntimeLocation, RuntimeLocationKind};
    use nostr::Keys;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    fn identity() -> AuthorityIdentity {
        let pubkey = PubkeyHex::parse(Keys::generate().public_key().to_hex()).unwrap();
        AuthorityIdentity {
            community_relay_url: "wss://relay.example.invalid".into(),
            logical_agent_pubkey: pubkey.clone(),
            executor_agent_pubkey: pubkey,
            task_id: Uuid::new_v4().to_string(),
            generation: Uuid::new_v4().to_string(),
            location: RuntimeLocation {
                kind: RuntimeLocationKind::Cloud,
                id: "runtime-b".into(),
            },
            runtime_support: RuntimeSupport::Portable,
        }
    }

    async fn host(
        authority: Arc<Mutex<ManagedAcpAuthority>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/v1/action",
            post(move |body: Bytes| {
                let authority = Arc::clone(&authority);
                async move {
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let response = serde_json::json!({
                        "type": "broker_result",
                        "protocolVersion": 1,
                        "requestId": request["requestId"],
                        "status": "succeeded",
                        "action": "authority.status",
                        "outcome": authority.lock().unwrap().clone(),
                    });
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::from(response.to_string()))
                        .unwrap()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}"), task)
    }

    #[tokio::test]
    async fn exact_active_binding_is_the_only_permit() {
        let expected = identity();
        let current = Arc::new(Mutex::new(ManagedAcpAuthority {
            identity: expected.clone(),
            state: AuthorityState::Active,
        }));
        let (url, _host) = host(Arc::clone(&current)).await;
        let fence = AuthorityFence::new(url, "credential".into(), expected.clone()).unwrap();
        assert_eq!(fence.permit().await, Ok(()));

        current.lock().unwrap().identity.location.id = "  runtime-b  ".into();
        assert_eq!(fence.permit().await, Ok(()));

        current.lock().unwrap().identity.generation = Uuid::new_v4().to_string();
        assert_eq!(fence.permit().await, Err(AuthorityError::BindingMismatch));
        current.lock().unwrap().identity = expected;
        current.lock().unwrap().state = AuthorityState::Quiescing;
        assert_eq!(fence.permit().await, Err(AuthorityError::NotActive));
        current.lock().unwrap().state = AuthorityState::Fenced;
        assert_eq!(fence.permit().await, Err(AuthorityError::NotActive));
    }

    #[tokio::test]
    async fn unsupported_runtime_and_host_loss_fail_closed() {
        let mut expected = identity();
        expected.runtime_support = RuntimeSupport::Unsupported;
        let current = Arc::new(Mutex::new(ManagedAcpAuthority {
            identity: expected.clone(),
            state: AuthorityState::Active,
        }));
        let (url, host) = host(current).await;
        let fence = AuthorityFence::new(url, "credential".into(), expected).unwrap();
        assert_eq!(
            fence.permit().await,
            Err(AuthorityError::UnsupportedRuntime)
        );
        host.abort();
        assert_eq!(fence.permit().await, Err(AuthorityError::HostUnavailable));
    }
}
