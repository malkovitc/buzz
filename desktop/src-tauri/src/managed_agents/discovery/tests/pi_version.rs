use super::super::{known_acp_runtime_exact, normalize_agent_args};

#[test]
fn pilot_runtime_uses_managed_adapter_without_legacy_acp_args() {
    let runtime = known_acp_runtime_exact("pi").expect("Pi pilot runtime should exist");
    assert_eq!(runtime.commands, &["pi-acp"]);
    assert_eq!(runtime.underlying_cli, Some("pi"));
    assert_eq!(runtime.adapter_install_commands.len(), 1);
    assert!(runtime.adapter_install_commands[0].contains("buzz-pi-acp-0.2.1.tgz"));
    assert_eq!(
        normalize_agent_args("pi-acp", vec!["acp".into()]),
        Vec::<String>::new()
    );
}

#[cfg(unix)]
#[test]
fn rejects_stale_and_accepts_brokered_release() {
    use std::os::unix::fs::PermissionsExt;

    use super::super::pi_runtime::pi_adapter_availability;
    use crate::managed_agents::AcpAvailabilityStatus;

    let dir = std::env::temp_dir().join(format!("buzz-probe-pi-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let bin = dir.join("pi-acp");
    std::fs::write(&bin, "#!/bin/sh\necho 'pi-acp 0.1.0'\n").expect("write stale script");
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).expect("chmod script");
    assert_eq!(
        pi_adapter_availability(&bin),
        AcpAvailabilityStatus::AdapterOutdated
    );

    std::fs::write(&bin, "#!/bin/sh\necho 'pi-acp 0.2.1'\n").expect("write current script");
    assert_eq!(
        pi_adapter_availability(&bin),
        AcpAvailabilityStatus::Available
    );
    let _ = std::fs::remove_dir_all(dir);
}
