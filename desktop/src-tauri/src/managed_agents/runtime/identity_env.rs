//! Local signer environment for legacy managed ACP children.

use std::process::Command;

use super::super::{resolve_command, ManagedAgentRecord};

pub(super) fn apply_direct_identity(
    command: &mut Command,
    record: &ManagedAgentRecord,
    relay_url: &str,
    direct_auth: bool,
) {
    if direct_auth {
        command.env("BUZZ_AGENT_MODE", "local");
        for key in [
            "BUZZ_BROKER_URL",
            "BUZZ_BROKER_CREDENTIAL",
            "BUZZ_BROKER_RELAY_URL",
            "BUZZ_MANAGED_ACP_AUTHORITY",
        ] {
            command.env_remove(key);
        }
        command.env("BUZZ_PRIVATE_KEY", &record.private_key_nsec);
        command.env("BUZZ_RELAY_URL", relay_url);
        if let Some(ref auth_tag) = record.auth_tag {
            command.env("BUZZ_AUTH_TAG", auth_tag);
        } else {
            command.env_remove("BUZZ_AUTH_TAG");
        }
    } else {
        command.env_remove("BUZZ_PRIVATE_KEY");
        command.env_remove("BUZZ_RELAY_URL");
        command.env_remove("BUZZ_AUTH_TAG");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_child_explicitly_stays_local_when_parent_has_broker_env() {
        let record = super::super::test_fixtures::fixture(
            super::super::super::RespondTo::OwnerOnly,
            Vec::new(),
            None,
        );
        let mut command = Command::new("buzz-acp");
        apply_direct_identity(&mut command, &record, "wss://relay.example", true);
        let env: std::collections::BTreeMap<_, _> = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();
        assert_eq!(env["BUZZ_AGENT_MODE"].as_deref(), Some("local"));
        assert!(env["BUZZ_PRIVATE_KEY"].is_some());
        for key in [
            "BUZZ_BROKER_URL",
            "BUZZ_BROKER_CREDENTIAL",
            "BUZZ_BROKER_RELAY_URL",
            "BUZZ_MANAGED_ACP_AUTHORITY",
        ] {
            assert_eq!(env.get(key), Some(&None));
        }
    }
}

pub(super) fn apply_git_credential(
    command: &mut Command,
    record: &ManagedAgentRecord,
    relay_url: &str,
    direct_auth: bool,
) {
    if !direct_auth {
        return;
    }
    let Some(cred_helper) = resolve_command("git-credential-nostr") else {
        eprintln!(
            "buzz-desktop: git-credential-nostr not found — agent {} will not have automatic Buzz git auth",
            record.name,
        );
        return;
    };
    let relay_http_url = crate::relay::relay_http_base_url(relay_url);
    command.env("NOSTR_PRIVATE_KEY", &record.private_key_nsec);
    command.env("GIT_TERMINAL_PROMPT", "0");
    command.env("GIT_CONFIG_COUNT", "2");
    command.env(
        "GIT_CONFIG_KEY_0",
        format!("credential.{relay_http_url}/git.helper"),
    );
    let helper = cred_helper.to_string_lossy().replace('\\', "/");
    command.env("GIT_CONFIG_VALUE_0", helper);
    command.env(
        "GIT_CONFIG_KEY_1",
        format!("credential.{relay_http_url}/git.useHttpPath"),
    );
    command.env("GIT_CONFIG_VALUE_1", "true");
}
