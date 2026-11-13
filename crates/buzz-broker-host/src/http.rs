use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
#[cfg(unix)]
use buzz_sdk::broker::{Action, ValidatedRequest};
use buzz_sdk::broker::{
    BrokerError, BrokerErrorCode, BrokerRequest, BrokerResponse, BrokerResult, BROKER_ACTION_PATH,
};
#[cfg(unix)]
use std::sync::Arc;

use crate::credential::CredentialDigest;
#[cfg(unix)]
use crate::publisher::RelayPublisher;
#[cfg(unix)]
use crate::signing::HostSigner;
#[cfg(unix)]
use crate::store::EffectAdmission;
use crate::store::{AuthorityStore, StoreError};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn app(store: AuthorityStore) -> Router {
    app_with_state(AppState {
        store,
        #[cfg(unix)]
        effects: None,
    })
}

#[cfg(unix)]
pub(crate) fn app_with_effects(
    store: AuthorityStore,
    signer: HostSigner,
    publisher: RelayPublisher,
) -> Router {
    app_with_state(AppState {
        store,
        effects: Some(Arc::new(EffectService { signer, publisher })),
    })
}

#[derive(Clone)]
struct AppState {
    store: AuthorityStore,
    #[cfg(unix)]
    effects: Option<Arc<EffectService>>,
}

#[cfg(unix)]
struct EffectService {
    signer: HostSigner,
    publisher: RelayPublisher,
}

fn app_with_state(state: AppState) -> Router {
    Router::new()
        .route(BROKER_ACTION_PATH, post(action))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
}

pub(crate) async fn serve(
    listener: tokio::net::TcpListener,
    store: AuthorityStore,
) -> Result<(), HostError> {
    let address = listener.local_addr().map_err(|_| HostError::Listener)?;
    if !address.ip().is_loopback() {
        return Err(HostError::NonLoopback);
    }
    axum::serve(listener, app(store))
        .await
        .map_err(|_| HostError::Server)
}

#[cfg(unix)]
pub(crate) async fn serve_with_effects(
    listener: tokio::net::TcpListener,
    store: AuthorityStore,
    signer: HostSigner,
) -> Result<(), HostError> {
    let address = listener.local_addr().map_err(|_| HostError::Listener)?;
    if !address.ip().is_loopback() {
        return Err(HostError::NonLoopback);
    }
    let publisher = RelayPublisher::new().map_err(|_| HostError::Publisher)?;
    axum::serve(listener, app_with_effects(store, signer, publisher))
        .await
        .map_err(|_| HostError::Server)
}

async fn action(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(()) => return empty_bad_request(),
    };
    let credential = match credential_from_headers(&headers) {
        Ok(credential) => credential,
        Err(()) => return broker_response(unauthenticated(request.request_id())),
    };
    #[cfg(unix)]
    if request.action() == Action::MessagePost {
        if let Some(effects) = &state.effects {
            return broker_response(
                effects
                    .execute_message_post(&state.store, &credential, &body, &request)
                    .await,
            );
        }
    }
    let response = match state.store.execute(&credential, &body, &request).await {
        Ok(response) => response,
        Err(StoreError::Unauthenticated) => unauthenticated(request.request_id()),
        Err(_) => internal_failure(request.request_id()),
    };
    broker_response(response)
}

#[cfg(unix)]
impl EffectService {
    async fn execute_message_post(
        &self,
        store: &AuthorityStore,
        credential: &CredentialDigest,
        body: &[u8],
        request: &ValidatedRequest,
    ) -> BrokerResponse {
        let mut publication_guard = None;
        let mut was_pending_before_lock = false;
        let admission = loop {
            match store
                .prepare_message_post(credential, body, request, &self.signer)
                .await
            {
                Ok(EffectAdmission::Pending(_, _, was_pending)) if publication_guard.is_none() => {
                    was_pending_before_lock = was_pending;
                    publication_guard =
                        match store.publication_lock_for_credential(credential).await {
                            Ok(guard) => Some(guard),
                            Err(_) => return internal_failure(request.request_id()),
                        };
                }
                Ok(admission) => break admission,
                Err(StoreError::Unauthenticated) => return unauthenticated(request.request_id()),
                Err(StoreError::AuthorityFenced) => {
                    return BrokerResponse::new(
                        request.request_id(),
                        BrokerResult::failed(BrokerError::new(
                            BrokerErrorCode::Unauthorized,
                            "authority generation is fenced",
                        )),
                    );
                }
                Err(StoreError::EffectsPending) => {
                    return BrokerResponse::new(
                        request.request_id(),
                        BrokerResult::failed(BrokerError::new(
                            BrokerErrorCode::ActionFailed,
                            "a prior effect is unresolved for this generation",
                        )),
                    );
                }
                Err(_) => return internal_failure(request.request_id()),
            }
        };
        let _publication_guard = publication_guard;
        match admission {
            EffectAdmission::Conflict => BrokerResponse::new(
                request.request_id(),
                BrokerResult::failed(BrokerError::new(
                    BrokerErrorCode::RequestIdConflict,
                    "requestId was already used with different request bytes",
                )),
            ),
            EffectAdmission::Completed(response) => response.replayed(),
            EffectAdmission::Pending(event, relay_url, _) => {
                let publication = if was_pending_before_lock {
                    self.publisher
                        .publish_pending(&self.signer, &relay_url, &event)
                        .await
                } else {
                    self.publisher
                        .publish(&self.signer, &relay_url, &event)
                        .await
                };
                if publication.is_err() {
                    return indeterminate(request.request_id());
                }
                store
                    .complete_message_post(credential, request.request_id(), &event.id.to_hex())
                    .await
                    .unwrap_or_else(|_| indeterminate(request.request_id()))
            }
        }
    }
}

#[cfg(unix)]
fn indeterminate(request_id: &str) -> BrokerResponse {
    BrokerResponse::new(
        request_id,
        BrokerResult::indeterminate(BrokerError::new(
            BrokerErrorCode::OutcomeUnknown,
            "relay publication outcome is not yet reconciled",
        )),
    )
}

fn parse_request(body: &[u8]) -> Result<buzz_sdk::broker::ValidatedRequest, ()> {
    serde_json::from_slice::<BrokerRequest>(body)
        .map_err(|_| ())?
        .validated()
        .map_err(|_| ())
}

fn credential_from_headers(headers: &HeaderMap) -> Result<CredentialDigest, ()> {
    let values = headers.get_all(axum::http::header::AUTHORIZATION);
    let mut iter = values.iter();
    let value = iter.next().ok_or(())?;
    if iter.next().is_some() {
        return Err(());
    }
    let bearer = value.to_str().map_err(|_| ())?;
    let token = bearer.strip_prefix("Bearer ").ok_or(())?;
    CredentialDigest::from_bearer(token).map_err(|_| ())
}

fn unauthenticated(request_id: &str) -> BrokerResponse {
    BrokerResponse::new(
        request_id,
        BrokerResult::failed(BrokerError::new(
            BrokerErrorCode::Unauthenticated,
            "broker credential is not authenticated",
        )),
    )
}

fn internal_failure(request_id: &str) -> BrokerResponse {
    BrokerResponse::new(
        request_id,
        BrokerResult::failed(BrokerError::new(
            BrokerErrorCode::Internal,
            "broker authority state is unavailable",
        )),
    )
}

fn broker_response(response: BrokerResponse) -> Response {
    match serde_json::to_vec(&response) {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap_or_else(|_| empty_internal_error()),
        Err(_) => empty_internal_error(),
    }
}

fn empty_bad_request() -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::empty())
        .unwrap_or_else(|_| empty_internal_error())
}

fn empty_internal_error() -> Response {
    Response::new(Body::empty())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum HostError {
    #[error("broker listener is unavailable")]
    Listener,
    #[error("broker host accepts loopback listeners only")]
    NonLoopback,
    #[error("broker HTTP server stopped unexpectedly")]
    Server,
    #[cfg(unix)]
    #[error("broker relay publisher is unavailable")]
    Publisher,
}
