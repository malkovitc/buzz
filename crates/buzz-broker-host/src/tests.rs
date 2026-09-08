use std::path::{Path, PathBuf};

use buzz_broker_client::{AuthorityError, AuthorityFence};
use buzz_sdk::broker::{
    ActionArgs, AuthorityIdentity, AuthorityStatusArgs, BrokerErrorCode, BrokerRequest,
    BrokerResponse, BrokerResult, ChannelReadArgs,
};
use tempfile::TempDir;

use crate::issuer::{fence_from_file, issue_to_file, IssuerError};
use crate::store::{AuthorityStore, StoreError};

const PUBKEY: &str = "a02c4e0850e5e612b4ddf95dbe2f5c56467cf27c6552203bc833ff438fb31971";
const TASK_ID: &str = "40b68c08-ed45-4c4b-a1d8-e46d6478d642";
const GENERATION: &str = "8d86e776-0f6a-418b-b7fb-4f87be556591";
const OTHER_GENERATION: &str = "22222222-2222-4222-8222-222222222222";
const CHANNEL_ID: &str = "5df7dfa8-e919-43df-8efd-f1dcb8af7071";

struct Fixture {
    directory: TempDir,
    state: PathBuf,
    authority_file: PathBuf,
    credential_file: PathBuf,
    identity: AuthorityIdentity,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        make_private_directory(directory.path());
        let state = directory.path().join("authority.db");
        let authority_file = directory.path().join("authority.json");
        let credential_file = directory.path().join("runtime.credential");
        let identity = identity(GENERATION);
        write_identity(&authority_file, &identity);
        Self {
            directory,
            state,
            authority_file,
            credential_file,
            identity,
        }
    }

    async fn issue(&self, store: &AuthorityStore) -> String {
        issue_to_file(store, &self.authority_file, &self.credential_file)
            .await
            .expect("authority issued");
        std::fs::read_to_string(&self.credential_file).expect("credential readable")
    }
}

#[cfg(unix)]
fn make_private_directory(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .expect("private test directory");
}

#[cfg(not(unix))]
fn make_private_directory(_path: &Path) {}

fn identity(generation: &str) -> AuthorityIdentity {
    serde_json::from_value(serde_json::json!({
        "communityRelayUrl": "WSS://Relay.Example:443/",
        "logicalAgentPubkey": PUBKEY.to_uppercase(),
        "executorAgentPubkey": PUBKEY,
        "taskId": TASK_ID.to_uppercase(),
        "generation": generation.to_uppercase(),
        "location": {"kind": "cloud", "id": "  runtime-b  "},
        "runtimeSupport": "portable"
    }))
    .expect("valid normalized identity")
}

fn write_identity(path: &Path, identity: &AuthorityIdentity) {
    std::fs::write(
        path,
        serde_json::to_vec(identity).expect("serializable identity"),
    )
    .expect("authority fixture written");
}

async fn spawn_host(store: AuthorityStore) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback listener");
    let address = listener.local_addr().expect("listener address");
    let task = tokio::spawn(async move {
        crate::http::serve(listener, store)
            .await
            .expect("host serves");
    });
    (format!("http://{address}"), task)
}

fn status_request(request_id: &str) -> buzz_sdk::broker::PreparedRequest {
    BrokerRequest::new(
        request_id,
        ActionArgs::AuthorityStatus(AuthorityStatusArgs {}),
    )
    .and_then(BrokerRequest::prepare)
    .expect("status request")
}

async fn send(base_url: &str, credential: &str, body: &[u8]) -> (reqwest::StatusCode, Vec<u8>) {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/v1/action"))
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(body.to_vec())
        .send()
        .await
        .expect("request sent");
    let status = response.status();
    let body = response.bytes().await.expect("response body").to_vec();
    (status, body)
}

#[tokio::test]
async fn exact_authority_is_active_then_durably_fenced_across_restart() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    assert_private_file(&fixture.credential_file);
    assert_state_omits_raw_credential(&fixture, &credential);

    let (base_url, host) = spawn_host(store.clone()).await;
    let fence = AuthorityFence::new(base_url, credential, fixture.identity.clone())
        .expect("fence configured");
    assert_eq!(fence.permit().await, Ok(()));

    fence_from_file(&store, &fixture.authority_file)
        .await
        .expect("authority fenced");
    fence_from_file(&store, &fixture.authority_file)
        .await
        .expect("duplicate fence is idempotent");
    assert_eq!(fence.permit().await, Err(AuthorityError::NotActive));
    host.abort();
    drop(store);

    let reopened = AuthorityStore::open(&fixture.state)
        .await
        .expect("store reopens");
    let (restarted_url, restarted_host) = spawn_host(reopened).await;
    let credential = std::fs::read_to_string(&fixture.credential_file).expect("credential remains");
    let restarted_fence = AuthorityFence::new(restarted_url, credential, fixture.identity.clone())
        .expect("restarted fence configured");
    assert_eq!(
        restarted_fence.permit().await,
        Err(AuthorityError::NotActive)
    );
    restarted_host.abort();
}

#[tokio::test]
async fn noncanonical_or_scope_mismatched_state_fails_closed() {
    for corruption in ["json", "scope", "generation"] {
        let fixture = Fixture::new();
        let store = AuthorityStore::open(&fixture.state)
            .await
            .expect("store opens");
        fixture.issue(&store).await;

        let pool = sqlx::SqlitePool::connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
        )
        .await
        .expect("corruption fixture opens");
        let statement = match corruption {
            "json" => "UPDATE broker_authorities SET authority_json = ' ' || authority_json",
            "scope" => "UPDATE broker_authorities SET authority_scope = zeroblob(32)",
            "generation" => {
                "UPDATE broker_authorities SET generation = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd'"
            }
            _ => unreachable!("closed corruption fixture"),
        };
        let update = sqlx::query(statement)
            .execute(&pool)
            .await
            .expect("corruption fixture written");
        assert_eq!(update.rows_affected(), 1);

        let fence = fence_from_file(&store, &fixture.authority_file)
            .await
            .expect_err("semantic corruption blocks fencing");
        assert!(matches!(fence, IssuerError::Store(StoreError::Integrity)));

        let mut changed_identity = fixture.identity.clone();
        changed_identity.location.id = "runtime-c".to_owned();
        let candidate = fixture.directory.path().join("candidate.json");
        let output = fixture.directory.path().join("candidate.credential");
        write_identity(&candidate, &changed_identity);
        let issue = issue_to_file(&store, &candidate, &output)
            .await
            .expect_err("semantic corruption blocks issuance");
        assert!(matches!(issue, IssuerError::Store(StoreError::Integrity)));
        assert!(!output.exists());
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM broker_authorities")
            .fetch_one(&pool)
            .await
            .expect("authority count");
        assert_eq!(count, 1);
        pool.close().await;

        drop(store);
        let reopened = AuthorityStore::open(&fixture.state).await;
        assert!(matches!(reopened, Err(StoreError::Integrity)));
    }
}

#[tokio::test]
async fn issuance_prevents_parallel_scope_and_fenced_generation_resurrection() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    fixture.issue(&store).await;

    let duplicate_output = fixture.directory.path().join("duplicate.credential");
    let duplicate = issue_to_file(&store, &fixture.authority_file, &duplicate_output)
        .await
        .expect_err("exact duplicate rejected");
    assert!(matches!(
        duplicate,
        IssuerError::Store(StoreError::GenerationAlreadyIssued)
    ));
    assert!(!duplicate_output.exists());

    let next_identity = identity(OTHER_GENERATION);
    let next_authority_file = fixture.directory.path().join("next-authority.json");
    let next_credential_file = fixture.directory.path().join("next.credential");
    write_identity(&next_authority_file, &next_identity);
    let wrong_fence = fence_from_file(&store, &next_authority_file)
        .await
        .expect_err("unissued identity cannot fence another generation");
    assert!(matches!(
        wrong_fence,
        IssuerError::Store(StoreError::AuthorityNotFound)
    ));
    let parallel = issue_to_file(&store, &next_authority_file, &next_credential_file)
        .await
        .expect_err("parallel active generation rejected");
    assert!(matches!(
        parallel,
        IssuerError::Store(StoreError::ScopeAlreadyActive)
    ));
    assert!(!next_credential_file.exists());

    fence_from_file(&store, &fixture.authority_file)
        .await
        .expect("first generation fenced");

    let mut changed_identity = fixture.identity.clone();
    changed_identity.location.id = "runtime-c".to_owned();
    let changed_authority_file = fixture.directory.path().join("changed-authority.json");
    let changed_credential_file = fixture.directory.path().join("changed.credential");
    write_identity(&changed_authority_file, &changed_identity);
    let changed_resurrection =
        issue_to_file(&store, &changed_authority_file, &changed_credential_file)
            .await
            .expect_err("generation cannot be resurrected by changing runtime metadata");
    assert!(matches!(
        changed_resurrection,
        IssuerError::Store(StoreError::GenerationAlreadyIssued)
    ));
    assert!(!changed_credential_file.exists());

    issue_to_file(&store, &next_authority_file, &next_credential_file)
        .await
        .expect("next generation issued after fence");

    let resurrection_output = fixture.directory.path().join("resurrection.credential");
    let resurrection = issue_to_file(&store, &fixture.authority_file, &resurrection_output)
        .await
        .expect_err("fenced generation cannot be reissued");
    assert!(matches!(
        resurrection,
        IssuerError::Store(StoreError::GenerationAlreadyIssued)
    ));
    assert!(!resurrection_output.exists());
}

#[tokio::test]
async fn request_receipts_replay_exact_bytes_and_reject_conflicts() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    let (base_url, host) = spawn_host(store.clone()).await;

    let request = status_request("same-id");
    let (_, first_body) = send(&base_url, &credential, request.body()).await;
    let first: BrokerResponse = serde_json::from_slice(&first_body).expect("first response");
    assert!(!first.replayed);
    assert!(matches!(first.result, BrokerResult::Succeeded { .. }));

    fence_from_file(&store, &fixture.authority_file)
        .await
        .expect("authority fenced");
    let (_, replay_body) = send(&base_url, &credential, request.body()).await;
    let replay: BrokerResponse = serde_json::from_slice(&replay_body).expect("replay response");
    assert!(replay.replayed);
    let replay_json = serde_json::to_value(&replay).expect("replay serializes");
    assert_eq!(replay_json["outcome"]["state"], "active");

    let conflicting = BrokerRequest::new(
        "same-id",
        ActionArgs::ChannelRead(ChannelReadArgs::channel(CHANNEL_ID)),
    )
    .and_then(BrokerRequest::prepare)
    .expect("conflicting request");
    let (_, conflict_body) = send(&base_url, &credential, conflicting.body()).await;
    let conflict: BrokerResponse =
        serde_json::from_slice(&conflict_body).expect("conflict response");
    assert!(matches!(
        conflict.result,
        BrokerResult::Failed { ref error }
            if error.code == BrokerErrorCode::RequestIdConflict
    ));

    let fresh = status_request("fresh-after-fence");
    let (_, fresh_body) = send(&base_url, &credential, fresh.body()).await;
    let fresh: BrokerResponse = serde_json::from_slice(&fresh_body).expect("fresh response");
    let fresh_json = serde_json::to_value(&fresh).expect("fresh serializes");
    assert_eq!(fresh_json["outcome"]["state"], "fenced");
    host.abort();
}

#[tokio::test]
async fn concurrent_duplicate_requests_commit_once_and_replay_one_verdict() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    let (base_url, host) = spawn_host(store).await;
    let body = status_request("concurrent-id").body().to_vec();

    let mut requests = Vec::new();
    for _ in 0..10 {
        let base_url = base_url.clone();
        let credential = credential.clone();
        let body = body.clone();
        requests.push(tokio::spawn(async move {
            let (status, body) = send(&base_url, &credential, &body).await;
            assert_eq!(status, reqwest::StatusCode::OK);
            serde_json::from_slice::<BrokerResponse>(&body).expect("concurrent response")
        }));
    }

    let mut responses = Vec::new();
    for request in requests {
        responses.push(request.await.expect("request task"));
    }
    assert!(responses
        .iter()
        .all(|response| matches!(response.result, BrokerResult::Succeeded { .. })));
    let replayed = responses
        .iter()
        .filter(|response| response.replayed)
        .count();
    assert_eq!((responses.len() - replayed, replayed), (1, 9));
    host.abort();
}

#[tokio::test]
async fn unknown_credentials_malformed_requests_and_non_loopback_fail_closed() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    let (base_url, host) = spawn_host(store.clone()).await;

    let unknown = crate::credential::generate();
    let request = status_request("unknown-credential");
    let (status, body) = send(&base_url, &unknown, request.body()).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let response: BrokerResponse = serde_json::from_slice(&body).expect("unauthenticated verdict");
    assert!(matches!(
        response.result,
        BrokerResult::Failed { ref error } if error.code == BrokerErrorCode::Unauthenticated
    ));

    let (status, body) = send(&base_url, &credential, br#"{"not":"a broker request"}"#).await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert!(body.is_empty());
    host.abort();

    let listener = tokio::net::TcpListener::bind("0.0.0.0:0")
        .await
        .expect("wildcard listener");
    let error = crate::http::serve(listener, store)
        .await
        .expect_err("non-loopback listener rejected");
    assert!(matches!(error, crate::http::HostError::NonLoopback));
}

#[tokio::test]
async fn malformed_authority_file_fails_before_credential_creation() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    std::fs::write(&fixture.authority_file, br#"{"unknown":true}"#)
        .expect("malformed authority fixture");
    let error = issue_to_file(&store, &fixture.authority_file, &fixture.credential_file)
        .await
        .expect_err("malformed identity rejected");
    assert!(matches!(error, IssuerError::AuthorityFile));
    assert!(!fixture.credential_file.exists());
}

#[tokio::test]
async fn credential_output_is_never_overwritten() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    std::fs::write(&fixture.credential_file, "preserve-me").expect("existing output");
    let error = issue_to_file(&store, &fixture.authority_file, &fixture.credential_file)
        .await
        .expect_err("existing credential output rejected");
    assert!(matches!(error, IssuerError::CredentialOutputExists));
    assert_eq!(
        std::fs::read_to_string(&fixture.credential_file).expect("existing output readable"),
        "preserve-me"
    );
}

#[cfg(unix)]
fn assert_private_file(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(path)
        .expect("credential metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);
}

#[cfg(not(unix))]
fn assert_private_file(path: &Path) {
    assert!(path.is_file());
}

fn assert_state_omits_raw_credential(fixture: &Fixture, credential: &str) {
    for suffix in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{}{suffix}", fixture.state.display()));
        if let Ok(bytes) = std::fs::read(path) {
            assert!(!bytes
                .windows(credential.len())
                .any(|window| window == credential.as_bytes()));
        }
    }
}
