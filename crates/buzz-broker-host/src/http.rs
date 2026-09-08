use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use buzz_sdk::broker::{
    BrokerError, BrokerErrorCode, BrokerRequest, BrokerResponse, BrokerResult, BROKER_ACTION_PATH,
};

use crate::credential::CredentialDigest;
use crate::store::{AuthorityStore, StoreError};

const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;

pub(crate) fn app(store: AuthorityStore) -> Router {
    Router::new()
        .route(BROKER_ACTION_PATH, post(action))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(store)
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

async fn action(State(store): State<AuthorityStore>, headers: HeaderMap, body: Bytes) -> Response {
    let request = match parse_request(&body) {
        Ok(request) => request,
        Err(()) => return empty_bad_request(),
    };
    let credential = match credential_from_headers(&headers) {
        Ok(credential) => credential,
        Err(()) => return broker_response(unauthenticated(request.request_id())),
    };
    let response = match store.execute(&credential, &body, &request).await {
        Ok(response) => response,
        Err(StoreError::Unauthenticated) => unauthenticated(request.request_id()),
        Err(_) => internal_failure(request.request_id()),
    };
    broker_response(response)
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
}
