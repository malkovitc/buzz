use super::*;

use axum::body::{to_bytes, Body};
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use buzz_sdk::broker::{PubkeyHex, RuntimeLocationKind};
use nostr::{EventBuilder, Keys, Kind, Tag};
use std::os::unix::fs::PermissionsExt as _;
use tower::ServiceExt as _;

#[tokio::test]
async fn huddle_grant_requires_one_fresh_owner_signature_for_one_local_generation() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    let agent_pubkey = PubkeyHex::parse(agent.public_key().to_hex()).expect("agent pubkey");
    fixture.identity.logical_agent_pubkey = agent_pubkey.clone();
    fixture.identity.executor_agent_pubkey = agent_pubkey;
    fixture.identity.location.kind = RuntimeLocationKind::Local;
    write_identity(&fixture.authority_file, &fixture.identity);

    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("agent key fixture");
    std::fs::write(
        &tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
            .expect("owner attestation"),
    )
    .expect("owner tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private signer fixture");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let bearer = fixture.issue(&store).await;
    let app = crate::http::app_with_effects(store, signer, RelayPublisher::new().unwrap());

    let challenge_request = Request::builder()
        .method("POST")
        .uri("/v1/huddle/challenge")
        .header(AUTHORIZATION, format!("Bearer {bearer}"))
        .body(Body::empty())
        .expect("challenge request");
    let challenge_response = app
        .clone()
        .oneshot(challenge_request)
        .await
        .expect("challenge response");
    assert_eq!(challenge_response.status(), StatusCode::OK);
    let challenge_body = to_bytes(challenge_response.into_body(), 4096)
        .await
        .expect("challenge body");
    let challenge: serde_json::Value =
        serde_json::from_slice(&challenge_body).expect("challenge JSON");
    let nonce = challenge["nonce"].as_str().expect("nonce");

    let target = serde_json::json!({
        "authority": fixture.identity,
        "channelId": CHANNEL_ID,
        "parentChannelId": "11111111-1111-4111-8111-111111111111",
        "huddleGeneration": 1,
    });
    let event = EventBuilder::new(Kind::Custom(27236), target.to_string())
        .tags([Tag::parse(["challenge", nonce]).expect("challenge tag")])
        .sign_with_keys(&owner)
        .expect("owner signs grant");
    let grant_body =
        serde_json::to_vec(&serde_json::json!({"ownerEvent": event})).expect("grant JSON");
    let grant_request = || {
        Request::builder()
            .method("POST")
            .uri("/v1/huddle/grant")
            .header(AUTHORIZATION, format!("Bearer {bearer}"))
            .header("content-type", "application/json")
            .body(Body::from(grant_body.clone()))
            .expect("grant request")
    };
    let accepted = app
        .clone()
        .oneshot(grant_request())
        .await
        .expect("grant response");
    assert_eq!(accepted.status(), StatusCode::OK);
    let accepted_body = to_bytes(accepted.into_body(), 4096)
        .await
        .expect("accepted body");
    let accepted_json: serde_json::Value =
        serde_json::from_slice(&accepted_body).expect("accepted JSON");
    assert!(accepted_json["grantId"].as_str().is_some());
    let replay = app
        .clone()
        .oneshot(grant_request())
        .await
        .expect("replay response");
    assert_eq!(replay.status(), StatusCode::FORBIDDEN);

    for case in [
        "wrong-owner",
        "wrong-authority",
        "bad-channel",
        "unknown-field",
        "stale",
    ] {
        let challenge_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/huddle/challenge")
                    .header(AUTHORIZATION, format!("Bearer {bearer}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(challenge_response.status(), StatusCode::OK, "{case}");
        let body = to_bytes(challenge_response.into_body(), 4096)
            .await
            .unwrap();
        let challenge: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let nonce = challenge["nonce"].as_str().unwrap();
        let mut target = target.clone();
        match case {
            "wrong-authority" => {
                target["authority"]["generation"] = serde_json::json!(OTHER_GENERATION)
            }
            "bad-channel" => target["channelId"] = serde_json::json!("not-a-uuid"),
            "unknown-field" => target["extra"] = serde_json::json!(true),
            _ => {}
        }
        let builder = EventBuilder::new(Kind::Custom(27236), target.to_string())
            .tags([Tag::parse(["challenge", nonce]).unwrap()]);
        let builder = if case == "stale" {
            builder.custom_created_at(nostr::Timestamp::from_secs(1))
        } else {
            builder
        };
        let signer = if case == "wrong-owner" {
            &Keys::generate()
        } else {
            &owner
        };
        let event = builder.sign_with_keys(signer).unwrap();
        let body = serde_json::to_vec(&serde_json::json!({"ownerEvent": event})).unwrap();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/huddle/grant")
                    .header(AUTHORIZATION, format!("Bearer {bearer}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{case}");
    }

    let reopened = AuthorityStore::open(&fixture.state).await.unwrap();
    crate::issuer::quiesce_from_file(&reopened, &fixture.authority_file)
        .await
        .unwrap();
    let after_quiesce = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/huddle/challenge")
                .header(AUTHORIZATION, format!("Bearer {bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_quiesce.status(), StatusCode::FORBIDDEN);

    let mut cloud_fixture = Fixture::new();
    cloud_fixture.identity.logical_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    cloud_fixture.identity.executor_agent_pubkey =
        cloud_fixture.identity.logical_agent_pubkey.clone();
    write_identity(&cloud_fixture.authority_file, &cloud_fixture.identity);
    let cloud_store = AuthorityStore::open(&cloud_fixture.state).await.unwrap();
    let cloud_bearer = cloud_fixture.issue(&cloud_store).await;
    let cloud_signer = HostSigner::load(&key_path, &tag_path).unwrap();
    let cloud_app =
        crate::http::app_with_effects(cloud_store, cloud_signer, RelayPublisher::new().unwrap());
    let cloud_challenge = cloud_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/huddle/challenge")
                .header(AUTHORIZATION, format!("Bearer {cloud_bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cloud_challenge.status(), StatusCode::FORBIDDEN);
}
