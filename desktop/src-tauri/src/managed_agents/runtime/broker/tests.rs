use super::*;
use std::collections::BTreeMap;

const AGENT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CHANNEL: &str = "d48aefc0-66d9-4eec-a5d2-9a2b0b5f4710";

#[test]
fn bundled_broker_binary_precedes_dev_and_path_candidates() {
    let directory = tempfile::tempdir().unwrap();
    let executable = directory.path().join("Buzz");
    let bundled = directory.path().join("buzz-broker-host");
    let dev = directory.path().join("untrusted-host");
    for path in [&bundled, &dev] {
        std::fs::write(path, b"binary").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    assert_eq!(
        trusted_broker_binary_at(&executable, Some(dev.as_os_str()), false).unwrap(),
        bundled
    );
    assert_eq!(
        trusted_broker_binary_at(&executable, Some(dev.as_os_str()), true).unwrap(),
        dev
    );
    let relative = std::ffi::OsStr::new("workspace/buzz-broker-host");
    assert!(trusted_broker_binary_at(&executable, Some(relative), true).is_err());
}

#[test]
fn only_exact_selected_pubkey_enters_broker_mode() {
    assert!(!selected_agent(None, AGENT).unwrap());
    assert!(!selected_agent(Some(OTHER), AGENT).unwrap());
    assert!(selected_agent(Some(AGENT), AGENT).unwrap());
    assert!(selected_agent(Some(&AGENT.to_uppercase()), AGENT).unwrap());
    assert!(selected_agent(Some("bad"), AGENT).is_err());
}

#[test]
fn selected_agent_requires_bundled_acp_owner_tag_and_saved_channels() {
    let mut env = BTreeMap::from([("BUZZ_ACP_CHANNELS".into(), CHANNEL.into())]);
    assert!(validate_canary_prerequisites(
        "buzz-acp",
        true,
        Some("tag"),
        Some(OTHER),
        "owner-only",
        &env
    )
    .is_ok());
    assert!(validate_canary_prerequisites(
        "custom-acp",
        true,
        Some("tag"),
        Some(OTHER),
        "owner-only",
        &env
    )
    .is_err());
    assert!(
        validate_canary_prerequisites("buzz-acp", true, None, Some(OTHER), "owner-only", &env)
            .is_err()
    );
    assert!(validate_canary_prerequisites(
        "buzz-acp",
        true,
        Some("tag"),
        Some(OTHER),
        "anyone",
        &env
    )
    .is_err());
    env.insert("BUZZ_ACP_CHANNELS".into(), "not-a-channel".into());
    assert!(validate_canary_prerequisites(
        "buzz-acp",
        true,
        Some("tag"),
        Some(OTHER),
        "owner-only",
        &env
    )
    .is_err());
}

#[test]
fn broker_child_env_tombstones_all_inherited_key_paths() {
    let mut command = std::process::Command::new("buzz-acp");
    apply_broker_child_env(
        &mut command,
        "http://127.0.0.1:34567",
        "bzh_test",
        "wss://relay.example",
        "{\"generation\":\"test\"}",
    );
    let env: BTreeMap<_, _> = command
        .get_envs()
        .map(|(key, value)| {
            (
                key.to_string_lossy().into_owned(),
                value.map(|v| v.to_string_lossy().into_owned()),
            )
        })
        .collect();
    for key in [
        "BUZZ_PRIVATE_KEY",
        "NOSTR_PRIVATE_KEY",
        "BUZZ_ACP_PRIVATE_KEY",
        "BUZZ_AUTH_TAG",
        "BUZZ_RELAY_URL",
    ] {
        assert_eq!(env.get(key), Some(&None), "{key} must be removed");
    }
    assert_eq!(env["BUZZ_AGENT_MODE"].as_deref(), Some("broker"));
    assert_eq!(env["BUZZ_BROKER_CREDENTIAL"].as_deref(), Some("bzh_test"));
}

fn launch_fixture() -> (
    tempfile::TempDir,
    ManagedAgentRecord,
    ManagedAgentRuntimeKey,
    String,
) {
    let directory = tempfile::tempdir().unwrap();
    let agent = nostr::Keys::generate();
    let owner = nostr::Keys::generate();
    let mut record = super::super::test_fixtures::fixture(
        super::super::super::RespondTo::OwnerOnly,
        Vec::new(),
        None,
    );
    record.pubkey = agent.public_key().to_hex();
    record.private_key_nsec = agent.secret_key().to_secret_hex();
    record.auth_tag = Some(
        buzz_sdk_pkg::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9").unwrap(),
    );
    record
        .env_vars
        .insert("BUZZ_ACP_CHANNELS".into(), CHANNEL.into());
    let key = ManagedAgentRuntimeKey::new(&record.pubkey, "ws://127.0.0.1:3000").unwrap();
    (directory, record, key, owner.public_key().to_hex())
}

#[test]
#[ignore = "requires BUZZ_TEST_BROKER_HOST_BIN pointing to a built buzz-broker-host"]
fn real_host_launch_stop_restart_and_serve_failure_rollback() {
    let binary = PathBuf::from(std::env::var("BUZZ_TEST_BROKER_HOST_BIN").unwrap());
    let (directory, record, key, owner) = launch_fixture();
    let base = directory.path().join("agents");
    ensure_private_dir(&base).unwrap();
    let log = directory.path().join("agent.log");
    let pair_dir = base.join("broker").join(key.runtime_id());
    let mut first =
        BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log).unwrap();
    let first_generation = first.authority_json.clone();
    let first_credential = first.credential.as_str().to_owned();
    assert!(pair_dir.join("current.json").exists());
    first.host.stop().unwrap();
    assert!(!pair_dir.join("current.json").exists());
    assert!(!first.host.generation_dir.exists());

    for partial_delete in [false, true] {
        // Crash after full or partial signer-directory removal but before
        // clearing current.json. The exact durable marker remains sufficient.
        write_current(&pair_dir.join("current.json"), first_generation.as_bytes()).unwrap();
        if partial_delete {
            ensure_private_dir(&first.host.generation_dir).unwrap();
            write_private_new(
                &first.host.generation_dir.join("agent.key"),
                record.private_key_nsec.as_bytes(),
            )
            .unwrap();
        }
        let mut after_cleanup_crash =
            BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log)
                .unwrap();
        assert_ne!(after_cleanup_crash.authority_json, first_generation);
        assert!(!first.host.generation_dir.exists());
        after_cleanup_crash.host.stop().unwrap();
    }

    let mut second =
        BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log).unwrap();
    assert_ne!(second.authority_json, first_generation);
    assert_ne!(second.credential.as_str(), first_credential);
    second.host.stop().unwrap();

    let wrapper = directory.path().join("fail-serve.sh");
    std::fs::write(
            &wrapper,
            b"#!/bin/sh\nif [ \"$3\" = serve ]; then exit 42; fi\nexec \"$BUZZ_TEST_BROKER_HOST_BIN\" \"$@\"\n",
        )
        .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        BrokerHost::launch(&base, &wrapper, &record, &key, &owner, "desktop-test", &log).is_err()
    );
    assert!(
        !pair_dir.join("current.json").exists(),
        "failed serve must roll back issued generation"
    );
    let mut after_failure =
        BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log).unwrap();
    let generation_dir = after_failure.host.generation_dir.clone();
    std::fs::set_permissions(&generation_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(after_failure.host.stop().is_err());
    assert!(
        pair_dir.join("current.json").exists(),
        "failed signer cleanup retains marker"
    );
    std::fs::set_permissions(&generation_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    after_failure.host.stop().unwrap();

    for contents in [None, Some(b"incomplete".as_slice())] {
        let empty_dir = pair_dir.join(format!("generation-{}", uuid::Uuid::new_v4()));
        ensure_private_dir(&empty_dir).unwrap();
        if let Some(contents) = contents {
            write_private_new(&empty_dir.join("authority.json"), contents).unwrap();
        }
        let mut recovered =
            BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log)
                .unwrap();
        assert!(
            !empty_dir.exists(),
            "pre-marker partial generation must be removed"
        );
        recovered.host.stop().unwrap();
    }

    for marker_written in [false, true] {
        let mut prior: AuthorityIdentity = serde_json::from_str(&first_generation).unwrap();
        prior.generation = uuid::Uuid::new_v4().to_string();
        let bytes = serde_json::to_vec(&prior).unwrap();
        let old_dir = pair_dir.join(format!("generation-{}", prior.generation));
        ensure_private_dir(&old_dir).unwrap();
        let old_authority = old_dir.join("authority.json");
        write_private_new(&old_authority, &bytes).unwrap();
        if marker_written {
            // Crash after marker, before writing either signer file.
            write_current(&pair_dir.join("current.json"), &bytes).unwrap();
        } else {
            // Recovery also cleans pre-marker files from the prior write order.
            write_private_new(
                &old_dir.join("agent.key"),
                record.private_key_nsec.as_bytes(),
            )
            .unwrap();
            write_private_new(
                &old_dir.join("owner.auth-tag"),
                record.auth_tag.as_deref().unwrap().as_bytes(),
            )
            .unwrap();
        }
        let mut recovered =
            BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log)
                .unwrap();
        assert!(!old_dir.exists(), "prior generation files must be removed");
        let inspect_file = directory
            .path()
            .join(format!("inspect-{}.json", prior.generation));
        write_private_new(&inspect_file, &bytes).unwrap();
        assert_eq!(
            run_cli(
                &binary,
                &pair_dir.join("authority.db"),
                "inspect",
                &[("--authority-file", &inspect_file)]
            )
            .unwrap()
            .trim(),
            "fenced"
        );
        recovered.host.stop().unwrap();
    }

    // The exited-child cleanup seam must keep its map entry and marker
    // when broker fencing or signer cleanup fails, so stop can retry.
    let launch =
        BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log).unwrap();
    let blocked_dir = launch.host.generation_dir.clone();
    let mut child = std::process::Command::new("/usr/bin/true").spawn().unwrap();
    child.wait().unwrap();
    let process = crate::managed_agents::ManagedAgentProcess {
        child,
        log_path: log.clone(),
        spawn_config: crate::managed_agents::spawn_snapshot::prospective_spawn_config_snapshot(
            &record,
            &[],
            &[],
            &key.relay_url,
            &Default::default(),
            false,
        ),
        setup_mode: false,
        adapter_availability: None,
        start_nonce: "broker-cleanup-test".into(),
        broker_host: Some(launch.host),
    };
    let mut runtimes = std::collections::HashMap::from([(
        key.clone(),
        crate::managed_agents::ManagedAgentPairRuntime::starting(process),
    )]);
    std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(
        crate::managed_agents::runtime_commands::close_tracked_broker(&mut runtimes, &key).is_err()
    );
    assert!(runtimes.contains_key(&key));
    assert!(pair_dir.join("current.json").exists());
    std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    crate::managed_agents::runtime_commands::close_tracked_broker(&mut runtimes, &key).unwrap();
    runtimes.remove(&key);
    assert!(!pair_dir.join("current.json").exists());

    let discarded =
        BrokerHost::launch(&base, &binary, &record, &key, &owner, "desktop-test", &log).unwrap();
    let blocked_dir = discarded.host.generation_dir.clone();
    let mut child = std::process::Command::new("/usr/bin/true").spawn().unwrap();
    child.wait().unwrap();
    let mut process = crate::managed_agents::ManagedAgentProcess {
        child,
        log_path: log,
        spawn_config: crate::managed_agents::spawn_snapshot::prospective_spawn_config_snapshot(
            &record,
            &[],
            &[],
            &key.relay_url,
            &Default::default(),
            false,
        ),
        setup_mode: false,
        adapter_availability: None,
        start_nonce: "discarded-broker-test".into(),
        broker_host: Some(discarded.host),
    };
    std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o500)).unwrap();
    assert!(crate::managed_agents::restore::close_discarded_spawn(&mut process).is_err());
    assert!(pair_dir.join("current.json").exists());
    std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
    drop(process);
    assert!(!pair_dir.join("current.json").exists());
}
