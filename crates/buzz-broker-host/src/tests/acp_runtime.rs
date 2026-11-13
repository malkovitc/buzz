use super::*;

use axum::{body::Bytes, routing::post, Router};
use buzz_sdk::broker::PubkeyHex;
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use std::{
    os::unix::fs::PermissionsExt as _,
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[tokio::test]
#[ignore = "requires built buzz-acp and buzz binaries; run with BUZZ_E2E_ACP_BIN and BUZZ_E2E_CLI_BIN"]
async fn actual_keyless_acp_reads_replies_and_loses_authority_after_fence() {
    let acp_bin = std::env::var("BUZZ_E2E_ACP_BIN").expect("absolute buzz-acp binary path");
    let cli_bin = std::env::var("BUZZ_E2E_CLI_BIN").expect("absolute buzz binary path");
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/fake-acp-post.mjs")
        .canonicalize()
        .expect("fake ACP fixture");
    let fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    let channel = uuid::Uuid::parse_str(CHANNEL_ID).unwrap();
    let owner_event = EventBuilder::new(Kind::Custom(9), "AE-ID:keyless-proof")
        .tags([
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
            Tag::parse(["p", &agent.public_key().to_hex()]).unwrap(),
        ])
        .sign_with_keys(&owner)
        .unwrap();
    let (post_tx, mut post_rx) = tokio::sync::mpsc::channel::<Event>(4);
    let query_event = owner_event.clone();
    let query_count = Arc::new(AtomicUsize::new(0));
    let relay_queries = Arc::clone(&query_count);
    let relay = Router::new()
        .route(
            "/query",
            post(move || {
                let event = query_event.clone();
                relay_queries.fetch_add(1, Ordering::SeqCst);
                async move { axum::Json(vec![event]) }
            }),
        )
        .route(
            "/events",
            post(move |body: Bytes| {
                let post_tx = post_tx.clone();
                async move {
                    let event: Event = serde_json::from_slice(&body).unwrap();
                    post_tx.send(event.clone()).await.unwrap();
                    axum::Json(serde_json::json!({
                        "accepted": true,
                        "event_id": event.id.to_hex()
                    }))
                }
            }),
        );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    let relay_task = tokio::spawn(async move { axum::serve(relay_listener, relay).await.unwrap() });

    let mut identity = fixture.identity.clone();
    identity.executor_agent_pubkey = PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    identity.logical_agent_pubkey = identity.executor_agent_pubkey.clone();
    identity.community_relay_url = relay_url.clone();
    write_identity(&fixture.authority_file, &identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).unwrap();
    std::fs::write(
        &tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9").unwrap(),
    )
    .unwrap();
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let signer = HostSigner::load(&key_path, &tag_path).unwrap();
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host_url = format!("http://{}", host_listener.local_addr().unwrap());
    let host_store = store.clone();
    let host_task = tokio::spawn(async move {
        axum::serve(
            host_listener,
            crate::http::app_with_effects(host_store, signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let authority_json = serde_json::to_string(&identity).unwrap();
    let acp_log_path = fixture.directory.path().join("acp.log");
    let acp_log = std::fs::File::create(&acp_log_path).unwrap();
    let acp_stdout_path = fixture.directory.path().join("acp.stdout");
    let acp_stdout = std::fs::File::create(&acp_stdout_path).unwrap();
    let mut child = tokio::process::Command::new(&acp_bin)
        .env("BUZZ_AGENT_MODE", "broker")
        .env("BUZZ_BROKER_URL", &host_url)
        .env("BUZZ_BROKER_CREDENTIAL", &bearer)
        .env("BUZZ_BROKER_RELAY_URL", &relay_url)
        .env("BUZZ_MANAGED_ACP_AUTHORITY", &authority_json)
        .env("BUZZ_ACP_CHANNELS", channel.to_string())
        .env("BUZZ_ACP_AGENT_OWNER", owner.public_key().to_hex())
        .env("BUZZ_ACP_AGENT_COMMAND", "node")
        .env("BUZZ_ACP_AGENT_ARGS", fixture_path)
        .env("BUZZ_BROKER_POLL_INTERVAL_MS", "100")
        .env("BUZZ_E2E_CLI_BIN", &cli_bin)
        .env("RUST_LOG", "buzz_acp=debug")
        .env_remove("BUZZ_PRIVATE_KEY")
        .env_remove("BUZZ_RELAY_URL")
        .env_remove("BUZZ_AUTH_TAG")
        .stdout(Stdio::from(acp_stdout))
        .stderr(Stdio::from(acp_log))
        .kill_on_drop(true)
        .spawn()
        .expect("actual keyless ACP starts");
    let reply = match tokio::time::timeout(Duration::from_secs(25), post_rx.recv()).await {
        Ok(Some(reply)) => reply,
        _ => {
            let mut log = std::fs::read_to_string(&acp_log_path).unwrap_or_default();
            log.push_str(&std::fs::read_to_string(&acp_stdout_path).unwrap_or_default());
            let selected: Vec<_> = log
                .lines()
                .take(35)
                .map(|line| line.replace(&bearer, "<redacted>"))
                .collect();
            panic!(
                "no ACP reply: relay_queries={}, child_status={:?}, log_bytes={}, acp_log={selected:?}",
                query_count.load(Ordering::SeqCst),
                child.try_wait().unwrap(),
                log.len()
            )
        }
    };
    assert!(reply.verify().is_ok());
    assert_eq!(reply.pubkey, agent.public_key());
    assert!(reply.content.contains("AE-ACK:keyless-proof"));
    let reply_owners: Vec<_> = reply
        .tags
        .iter()
        .filter_map(|tag| {
            let encoded = serde_json::to_string(tag).ok()?;
            buzz_sdk::nip_oa::verify_auth_tag(&encoded, &reply.pubkey).ok()
        })
        .collect();
    assert_eq!(reply_owners, vec![owner.public_key()]);
    assert!(reply
        .tags
        .iter()
        .any(|tag| { tag.kind().to_string() == "h" && tag.content() == Some(CHANNEL_ID) }));
    assert!(reply.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.first().map(String::as_str) == Some("e")
            && parts.get(1).map(String::as_str) == Some(owner_event.id.to_hex().as_str())
            && parts.get(3).map(String::as_str) == Some("reply")
    }));

    assert!(
        child.try_wait().unwrap().is_none(),
        "runtime is live before quiesce"
    );
    store.quiesce(&identity).await.unwrap();
    let exit = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("quiesced runtime stops promptly")
        .expect("runtime wait");
    assert!(exit.success(), "runtime exits cleanly after authority loss");
    store.fence(&identity).await.unwrap();
    let cli_output = tokio::process::Command::new(cli_bin)
        .args([
            "messages",
            "send",
            "--channel",
            CHANNEL_ID,
            "--content",
            "late",
        ])
        .env("BUZZ_AGENT_MODE", "broker")
        .env("BUZZ_BROKER_URL", &host_url)
        .env("BUZZ_BROKER_CREDENTIAL", &bearer)
        .env("BUZZ_MANAGED_ACP_AUTHORITY", &authority_json)
        .env_remove("BUZZ_PRIVATE_KEY")
        .stdout(Stdio::null())
        .output()
        .await
        .unwrap();
    assert!(
        !cli_output.status.success(),
        "fenced credential cannot publish"
    );
    let cli_error: serde_json::Value = serde_json::from_slice(&cli_output.stderr).unwrap();
    assert_eq!(
        cli_error["message"],
        "managed ACP broker action: managed ACP generation is not active"
    );
    assert!(post_rx.try_recv().is_err(), "no post after fence");
    host_task.abort();
    relay_task.abort();
}
