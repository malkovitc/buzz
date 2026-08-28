use std::path::Path;

use super::{probe_codex_acp_version, KnownAcpRuntime, BUZZ_AGENT_AVATAR_URL};
use crate::managed_agents::AcpAvailabilityStatus;

pub(super) const PI_RUNTIME: KnownAcpRuntime = KnownAcpRuntime {
    id: "pi",
    label: "Pi (pilot)",
    commands: &["pi-acp"],
    aliases: &[],
    avatar_url: BUZZ_AGENT_AVATAR_URL,
    mcp_command: None,
    mcp_hooks: false,
    underlying_cli: Some("pi"),
    cli_install_commands: &["npm install -g @earendil-works/pi-coding-agent@0.84.2"],
    cli_install_commands_windows: &[],
    adapter_install_commands: &["npm install -g https://github.com/malkovitc/buzz/releases/download/pi-acp-v0.2.3/buzz-pi-acp-0.2.3.tgz"],
    cli_install_instructions_url: "https://github.com/badlogic/pi-mono",
    adapter_install_instructions_url: "https://github.com/malkovitc/buzz/tree/pi-acp-v0.2.3/tools/pi-acp",
    cli_install_hint: "Install Pi 0.84.2; the pi-acp pilot adapter is packaged separately.",
    adapter_install_hint: "Install the reviewed pi-acp 0.2.3 pilot package before selecting this runtime.",
    skill_dir: None,
    supports_acp_model_switching: false,
    model_env_var: None,
    provider_env_var: None,
    provider_locked: true,
    default_env: &[],
    config_file_path: Some("~/.pi/agent/settings.json"),
    config_file_format: Some("json"),
    supports_acp_native_config: false,
    thinking_env_var: None,
    max_tokens_env_var: None,
    context_limit_env_var: None,
    max_rounds_env_var: None,
    required_normalized_fields: &[],
    login_hint: Some("Run Pi interactively once to configure provider authentication."),
    auth_probe_args: None,
};

/// Older Pi pilot adapters lack durable receipt and output-drain guarantees.
const MIN_PI_ACP_VERSION: (u64, u64, u64) = (0, 2, 3);

pub(super) fn pi_adapter_availability(path: &Path) -> AcpAvailabilityStatus {
    match probe_codex_acp_version(path) {
        Some(version) if version >= MIN_PI_ACP_VERSION => AcpAvailabilityStatus::Available,
        _ => AcpAvailabilityStatus::AdapterOutdated,
    }
}

pub(super) fn gate_adapter(
    availability: AcpAvailabilityStatus,
    command: Option<&str>,
    binary_path: Option<&str>,
) -> AcpAvailabilityStatus {
    if availability == AcpAvailabilityStatus::Available && command == Some("pi-acp") {
        binary_path
            .map(Path::new)
            .map(pi_adapter_availability)
            .unwrap_or(AcpAvailabilityStatus::AdapterOutdated)
    } else {
        availability
    }
}
