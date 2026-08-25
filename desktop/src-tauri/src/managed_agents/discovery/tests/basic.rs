use super::super::managed_agent_avatar_url;

#[test]
fn returns_none_for_unknown_commands() {
    assert!(managed_agent_avatar_url("custom-agent").is_none());
}
