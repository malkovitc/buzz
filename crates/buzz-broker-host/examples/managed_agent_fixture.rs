//! Synthetic identities for the opt-in container smoke. Never use these for production.

#[cfg(unix)]
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
#[cfg(unix)]
use std::path::Path;

#[cfg(unix)]
use nostr::Keys;

#[cfg(unix)]
fn private_file(path: &Path, contents: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = args
        .next()
        .ok_or("usage: managed_agent_fixture DIR RELAY_URL")?;
    let relay_url = args.next().ok_or("missing relay URL")?;
    if args.next().is_some() {
        return Err("unexpected argument".into());
    }
    let root = Path::new(&root);
    for name in ["state", "signing", "credential", "workspace"] {
        let path = root.join(name);
        fs::create_dir(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
    }
    let agent = Keys::generate();
    let owner = Keys::generate();
    private_file(
        &root.join("signing/agent.key"),
        &agent.secret_key().to_secret_hex(),
    )?;
    private_file(
        &root.join("signing/owner.auth-tag"),
        &buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")?,
    )?;
    let authority = serde_json::json!({
        "communityRelayUrl": relay_url.to_string_lossy(),
        "logicalAgentPubkey": agent.public_key().to_hex(),
        "executorAgentPubkey": agent.public_key().to_hex(),
        "taskId": uuid::Uuid::new_v4(),
        "generation": uuid::Uuid::new_v4(),
        "location": {"kind": "cloud", "id": "container-smoke"},
        "runtimeSupport": "portable"
    });
    private_file(&root.join("authority.json"), &authority.to_string())?;
    private_file(&root.join("owner.pubkey"), &owner.public_key().to_hex())?;
    Ok(())
}

#[cfg(not(unix))]
fn main() {
    eprintln!("managed-agent fixture generation requires Unix");
}
