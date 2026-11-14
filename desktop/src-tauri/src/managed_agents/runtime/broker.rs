use std::collections::BTreeMap;
use std::io::Write as _;
use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use buzz_sdk_pkg::broker::{
    AuthorityIdentity, PubkeyHex, RuntimeLocation, RuntimeLocationKind, RuntimeSupport,
};
use zeroize::Zeroizing;

use super::super::{ManagedAgentRecord, ManagedAgentRuntimeKey};

const CLI_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(60);
const LISTENER_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn reconcile_pair(base_dir: &Path, key: &ManagedAgentRuntimeKey) -> Result<(), String> {
    let pair_dir = base_dir.join("broker").join(key.runtime_id());
    let current = pair_dir.join("current.json");
    let entries = match std::fs::read_dir(&pair_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("cannot scan prior broker generations".into()),
    };
    let mut has_work = false;
    for entry in entries {
        let entry = entry.map_err(|_| "cannot inspect prior broker generation")?;
        let name = entry.file_name();
        has_work |= name == "current.json" || name.to_string_lossy().starts_with("generation-");
    }
    if !has_work {
        return Ok(());
    }
    let binary = trusted_broker_binary()?;
    let task_id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("buzz-desktop-broker:{}", key.runtime_id()).as_bytes(),
    )
    .to_string();
    reconcile_previous(
        &binary,
        &pair_dir.join("authority.db"),
        &current,
        &pair_dir,
        key,
        &task_id,
    )?;
    reconcile_orphans(
        &binary,
        &pair_dir.join("authority.db"),
        &pair_dir,
        key,
        &task_id,
    )
}

fn trusted_broker_binary() -> Result<PathBuf, String> {
    let executable = std::env::current_exe().map_err(|_| "cannot locate Desktop executable")?;
    let dev_override = std::env::var_os("BUZZ_DESKTOP_BROKER_HOST_DEV_PATH");
    trusted_broker_binary_at(&executable, dev_override.as_deref(), cfg!(debug_assertions))
}

fn trusted_broker_binary_at(
    executable: &Path,
    dev_override: Option<&std::ffi::OsStr>,
    allow_dev_override: bool,
) -> Result<PathBuf, String> {
    let bundled = executable
        .parent()
        .ok_or("Desktop executable has no parent directory")?
        .join("buzz-broker-host");
    let binary = if allow_dev_override {
        dev_override.map(PathBuf::from).unwrap_or(bundled)
    } else {
        bundled
    };
    let metadata = std::fs::symlink_metadata(&binary)
        .map_err(|_| "broker canary requires the bundled buzz-broker-host binary")?;
    if !binary.is_absolute()
        || !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o022 != 0
        || metadata.permissions().mode() & 0o111 == 0
    {
        return Err("broker host binary path is not trusted".into());
    }
    Ok(binary)
}

pub(super) fn selected_for_spawn(
    record: &ManagedAgentRecord,
    owner_hex: Option<&str>,
    env: &BTreeMap<String, String>,
) -> Result<bool, String> {
    let selector = match std::env::var("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY is not valid text".into());
        }
    };
    let selected = selected_agent(selector.as_deref(), &record.pubkey)?;
    if selected {
        let (gate_set, _) = super::build_respond_to_env(record, owner_hex)?;
        let respond_to = gate_set
            .iter()
            .find(|(key, _)| *key == "BUZZ_ACP_RESPOND_TO")
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        validate_canary_prerequisites(
            &record.acp_command,
            record.backend == super::super::BackendKind::Local,
            record.auth_tag.as_deref(),
            owner_hex,
            respond_to,
            env,
        )?;
    }
    Ok(selected)
}

pub(super) fn prepare_child(
    command: &mut Command,
    app: &tauri::AppHandle,
    record: &ManagedAgentRecord,
    runtime_key: &ManagedAgentRuntimeKey,
    owner_hex: Option<&str>,
    log_path: &Path,
    selected: bool,
) -> Result<Option<BrokerLaunch>, String> {
    if !selected {
        return Ok(None);
    }
    let binary = trusted_broker_binary()?;
    let owner = owner_hex.ok_or("broker canary requires the workspace owner")?;
    let launch = BrokerHost::launch(
        &super::super::managed_agents_base_dir(app)?,
        &binary,
        record,
        runtime_key,
        owner,
        &super::current_instance_id(app),
        log_path,
    )?;
    apply_broker_child_env(
        command,
        &launch.url,
        launch.credential.as_str(),
        &runtime_key.relay_url,
        &launch.authority_json,
    );
    Ok(Some(launch))
}

pub(super) struct BrokerLaunch {
    pub host: BrokerHost,
    pub url: String,
    pub credential: Zeroizing<String>,
    pub authority_json: String,
}

pub(crate) struct BrokerHost {
    child: Option<Child>,
    binary: PathBuf,
    state: PathBuf,
    authority: PathBuf,
    authority_json: String,
    current: PathBuf,
    generation_dir: PathBuf,
    issued: bool,
    fenced: bool,
    closed: bool,
}

pub(crate) struct BrokerMediaSnapshot {
    pub(crate) url: String,
    pub(crate) credential: Zeroizing<String>,
    pub(crate) authority_json: String,
}

impl std::fmt::Debug for BrokerHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BrokerHost")
            .field("generation_dir", &self.generation_dir)
            .field("issued", &self.issued)
            .field("fenced", &self.fenced)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl BrokerHost {
    pub(crate) fn with_current_media<R>(
        &mut self,
        expected_authority: &str,
        register: impl FnOnce() -> R,
    ) -> Result<R, String> {
        let current = self.media_snapshot()?;
        if current.authority_json != expected_authority {
            return Err("broker generation changed during media setup".into());
        }
        Ok(register())
    }

    pub(crate) fn media_snapshot(&mut self) -> Result<BrokerMediaSnapshot, String> {
        if self.closed || self.fenced || !self.issued {
            return Err("broker generation is not active for media".into());
        }
        if self
            .child
            .as_mut()
            .ok_or("broker host is not running")?
            .try_wait()
            .map_err(|_| "cannot inspect broker host")?
            .is_some()
        {
            return Err("broker host is not running".into());
        }
        if std::fs::read(&self.current).map_err(|_| "cannot read broker current authority")?
            != self.authority_json.as_bytes()
        {
            return Err("broker generation is no longer current".into());
        }
        let credential = Zeroizing::new(
            std::fs::read_to_string(self.generation_dir.join("credential"))
                .map_err(|_| "cannot read broker media credential")?,
        );
        if credential.is_empty() || credential.trim() != credential.as_str() {
            return Err("broker media credential is malformed".into());
        }
        let address: std::net::SocketAddr =
            std::fs::read_to_string(self.generation_dir.join("listen.addr"))
                .map_err(|_| "cannot read broker media listener")?
                .trim()
                .parse()
                .map_err(|_| "broker media listener is malformed")?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err("broker media listener is not loopback".into());
        }
        Ok(BrokerMediaSnapshot {
            url: format!("http://{address}"),
            credential,
            authority_json: self.authority_json.clone(),
        })
    }

    pub(super) fn launch(
        base_dir: &Path,
        binary: &Path,
        record: &ManagedAgentRecord,
        key: &ManagedAgentRuntimeKey,
        owner_hex: &str,
        desktop_instance_id: &str,
        log_path: &Path,
    ) -> Result<BrokerLaunch, String> {
        let pair_dir = base_dir.join("broker").join(key.runtime_id());
        ensure_private_dir(&base_dir.join("broker"))?;
        ensure_private_dir(&pair_dir)?;
        let state = pair_dir.join("authority.db");
        let current = pair_dir.join("current.json");
        let task_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("buzz-desktop-broker:{}", key.runtime_id()).as_bytes(),
        )
        .to_string();
        reconcile_previous(binary, &state, &current, &pair_dir, key, &task_id)?;
        reconcile_orphans(binary, &state, &pair_dir, key, &task_id)?;

        let generation = uuid::Uuid::new_v4().to_string();
        let agent_pubkey = nostr::PublicKey::from_hex(&key.pubkey)
            .map_err(|_| "broker canary agent pubkey is invalid")?;
        let signer_keys = nostr::Keys::parse(record.private_key_nsec.trim())
            .map_err(|_| "broker canary agent signer is invalid")?;
        if signer_keys.public_key() != agent_pubkey {
            return Err("broker canary signer does not match the selected agent".into());
        }
        let auth_tag = record
            .auth_tag
            .as_deref()
            .ok_or("broker canary requires a signed NIP-OA owner attestation")?;
        let attested_owner = buzz_sdk_pkg::nip_oa::verify_auth_tag(auth_tag, &agent_pubkey)
            .map_err(|_| "broker canary owner attestation is invalid")?;
        if attested_owner.to_hex() != owner_hex {
            return Err("broker canary attestation belongs to another owner".into());
        }
        let identity = AuthorityIdentity {
            community_relay_url: key.relay_url.clone(),
            logical_agent_pubkey: PubkeyHex::parse(&key.pubkey).map_err(|e| e.to_string())?,
            executor_agent_pubkey: PubkeyHex::parse(&key.pubkey).map_err(|e| e.to_string())?,
            task_id,
            generation: generation.clone(),
            location: RuntimeLocation {
                kind: RuntimeLocationKind::Local,
                id: desktop_instance_id.to_owned(),
            },
            runtime_support: RuntimeSupport::Unsupported,
        }
        .validated()
        .map_err(|e| e.to_string())?;
        let authority_json = serde_json::to_string(&identity).map_err(|e| e.to_string())?;
        let generation_dir = pair_dir.join(format!("generation-{generation}"));
        ensure_private_dir(&generation_dir)?;
        let authority = generation_dir.join("authority.json");
        let credential_file = generation_dir.join("credential");
        let signer_file = generation_dir.join("agent.key");
        let auth_tag_file = generation_dir.join("owner.auth-tag");
        let listen_file = generation_dir.join("listen.addr");
        let mut host = Self {
            child: None,
            binary: binary.to_path_buf(),
            state,
            authority,
            authority_json: authority_json.clone(),
            current,
            generation_dir,
            issued: false,
            fenced: false,
            closed: false,
        };
        write_private_new(&host.authority, authority_json.as_bytes())?;
        // The durable marker must exist before either plaintext signer file.
        write_current(&host.current, authority_json.as_bytes())?;
        write_private_new(&signer_file, record.private_key_nsec.as_bytes())?;
        write_private_new(&auth_tag_file, auth_tag.as_bytes())?;
        if let Err(error) = host.run_cli(
            "issue",
            &[
                ("--authority-file", &host.authority),
                ("--credential-output", &credential_file),
            ],
        ) {
            // The output file is written before the DB commit. Its presence
            // means an interrupted CLI may have issued authority: retain the
            // durable current identity and attempt exact rollback.
            host.issued = credential_file.exists();
            return Err(error);
        }
        host.issued = true;
        let credential = Zeroizing::new(
            std::fs::read_to_string(&credential_file)
                .map_err(|_| "broker credential output is unreadable".to_string())?,
        );
        if credential.trim() != credential.as_str() || credential.is_empty() {
            return Err("broker credential output is malformed".into());
        }

        let stderr = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(log_path)
            .map_err(|_| "cannot open broker host log".to_string())?;
        let child = Command::new(binary)
            .args([
                "--state",
                host.state.to_str().ok_or("invalid broker state path")?,
                "serve",
                "--bind",
                "127.0.0.1:0",
                "--listen-output",
                listen_file.to_str().ok_or("invalid broker listener path")?,
                "--exit-on-stdin-eof",
                "--signing-key-file",
                signer_file.to_str().ok_or("invalid broker signer path")?,
                "--auth-tag-file",
                auth_tag_file
                    .to_str()
                    .ok_or("invalid broker attestation path")?,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|_| "failed to spawn broker host".to_string())?;
        host.child = Some(child);
        let address = host.wait_for_listener(&listen_file)?;
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err("broker host published a non-loopback listener".into());
        }
        let url = format!("http://{address}");
        // The host binds before publishing this address; a successful connect
        // also confirms it did not exit immediately after loading the signer.
        std::net::TcpStream::connect_timeout(&address, Duration::from_secs(1))
            .map_err(|_| "broker host listener is unreachable".to_string())?;
        if host
            .child
            .as_mut()
            .is_some_and(|child| child.try_wait().ok().flatten().is_some())
        {
            return Err("broker host exited during startup".into());
        }
        Ok(BrokerLaunch {
            host,
            url,
            credential,
            authority_json,
        })
    }

    fn wait_for_listener(&mut self, path: &Path) -> Result<std::net::SocketAddr, String> {
        let deadline = Instant::now() + LISTENER_TIMEOUT;
        loop {
            if let Ok(bytes) = std::fs::read_to_string(path) {
                // The host creates this file before writing and syncing the
                // address. Keep polling through that short partial-file window.
                if let Ok(address) = bytes.trim().parse() {
                    return Ok(address);
                }
            }
            if self
                .child
                .as_mut()
                .is_some_and(|child| child.try_wait().ok().flatten().is_some())
            {
                return Err("broker host exited before publishing its listener".into());
            }
            if Instant::now() >= deadline {
                return Err("broker host listener startup timed out".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Close media admission, reconcile pending signed effects, then fence the
    /// exact generation before releasing the signer. Failure leaves the host
    /// and durable files available for a retry.
    pub(crate) fn stop(&mut self) -> Result<(), String> {
        if self.closed {
            return Ok(());
        }
        let marker_matches = match std::fs::read(&self.current) {
            Ok(current) => current == self.authority_json.as_bytes(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return Err("cannot read broker current authority".into()),
        };
        if (self.issued || marker_matches) && !self.fenced {
            recover_and_close(
                &self.binary,
                &self.state,
                &self.authority,
                &self.generation_dir,
            )?;
            self.fenced = true;
        }
        self.reap_child();
        remove_generation_dir(&self.generation_dir)?;
        if marker_matches {
            std::fs::remove_file(&self.current)
                .map_err(|_| "cannot clear fenced broker current authority")?;
        }
        self.closed = true;
        Ok(())
    }

    fn reap_child(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.stdin.take());
            let deadline = Instant::now() + Duration::from_secs(1);
            while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }

    fn run_cli(&self, verb: &str, args: &[(&str, &Path)]) -> Result<String, String> {
        run_cli(&self.binary, &self.state, verb, args)
    }
}

impl Drop for BrokerHost {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("buzz-desktop: broker generation retained for recovery: {error}");
            self.reap_child();
        }
    }
}

fn reconcile_previous(
    binary: &Path,
    state: &Path,
    current: &Path,
    pair_dir: &Path,
    key: &ManagedAgentRuntimeKey,
    task_id: &str,
) -> Result<(), String> {
    let bytes = match std::fs::read(current) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => return Err("cannot read prior broker authority".into()),
    };
    let identity: AuthorityIdentity =
        serde_json::from_slice(&bytes).map_err(|_| "prior broker authority is malformed")?;
    validate_prior_identity(&identity, key, task_id)?;
    let generation_dir = pair_dir.join(format!("generation-{}", identity.generation));
    let authority_path = generation_dir.join("authority.json");
    let authority = match std::fs::read(&authority_path) {
        Ok(generation_bytes) if generation_bytes == bytes => authority_path.as_path(),
        Ok(_) => return Err("prior broker marker does not match generation identity".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => current,
        Err(_) => return Err("cannot read prior broker generation identity".into()),
    };
    recover_and_close(binary, state, authority, &generation_dir)?;
    remove_generation_dir(&generation_dir)?;
    std::fs::remove_file(current).map_err(|_| "cannot clear fenced broker authority")?;
    Ok(())
}

fn validate_prior_identity(
    identity: &AuthorityIdentity,
    key: &ManagedAgentRuntimeKey,
    task_id: &str,
) -> Result<(), String> {
    identity
        .clone()
        .validated()
        .map_err(|_| "prior broker authority identity is invalid")?;
    if identity.community_relay_url != key.relay_url
        || identity.logical_agent_pubkey.as_str() != key.pubkey
        || identity.executor_agent_pubkey.as_str() != key.pubkey
        || identity.task_id != task_id
        || identity.location.kind != RuntimeLocationKind::Local
    {
        return Err("prior broker authority does not match this runtime pair".into());
    }
    Ok(())
}

fn reconcile_orphans(
    binary: &Path,
    state: &Path,
    pair_dir: &Path,
    key: &ManagedAgentRuntimeKey,
    task_id: &str,
) -> Result<(), String> {
    for entry in std::fs::read_dir(pair_dir).map_err(|_| "cannot scan broker generations")? {
        let entry = entry.map_err(|_| "cannot inspect broker generation")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err("broker generation name is invalid".into());
        };
        if !name.starts_with("generation-") {
            continue;
        }
        let generation_dir = entry.path();
        let metadata = std::fs::symlink_metadata(&generation_dir)
            .map_err(|_| "cannot inspect broker generation")?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err("broker generation is not a real directory".into());
        }
        let authority = generation_dir.join("authority.json");
        let identity: AuthorityIdentity = match std::fs::read(&authority)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        {
            Some(identity) => identity,
            None if orphan_has_only_identity_file(&generation_dir)? => {
                // New write order leaves no signer or bearer before a valid
                // authority and current marker are durable.
                remove_generation_dir(&generation_dir)?;
                continue;
            }
            None => return Err("orphan broker identity is missing or malformed".into()),
        };
        validate_prior_identity(&identity, key, task_id)?;
        if name != format!("generation-{}", identity.generation) {
            return Err("orphan broker generation name does not match identity".into());
        }
        recover_and_close(binary, state, &authority, &generation_dir)?;
        remove_generation_dir(&generation_dir)?;
    }
    Ok(())
}

fn orphan_has_only_identity_file(generation_dir: &Path) -> Result<bool, String> {
    for entry in std::fs::read_dir(generation_dir).map_err(|_| "cannot scan orphan broker files")? {
        let entry = entry.map_err(|_| "cannot inspect orphan broker file")?;
        if entry.file_name() != "authority.json" {
            return Ok(false);
        }
    }
    Ok(true)
}

fn remove_generation_dir(path: &Path) -> Result<(), String> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("cannot remove fenced broker signer files".into()),
    }
}

fn recover_and_close(
    binary: &Path,
    state: &Path,
    authority: &Path,
    generation_dir: &Path,
) -> Result<(), String> {
    let inspect = || run_cli(binary, state, "inspect", &[("--authority-file", authority)]);
    if inspect()?.trim() == "missing" {
        let recovery_credential =
            generation_dir.join(format!("recovery-{}.credential", uuid::Uuid::new_v4()));
        if run_cli(
            binary,
            state,
            "issue",
            &[
                ("--authority-file", authority),
                ("--credential-output", &recovery_credential),
            ],
        )
        .is_err()
            && inspect()?.trim() == "missing"
        {
            return Err("broker authority issue is indeterminate; marker retained".into());
        }
    }
    // The second inspection also detects a crash or a concurrent issue that
    // raced the recovery CLI. Never delete a marker on indeterminate state.
    match inspect()?.trim() {
        "active" | "quiescing" | "fenced" => {
            close_authority(binary, state, authority, generation_dir)
        }
        _ => Err("broker authority state is indeterminate; marker retained".into()),
    }
}

fn close_authority(
    binary: &Path,
    state: &Path,
    authority: &Path,
    generation_dir: &Path,
) -> Result<(), String> {
    let deadline = Instant::now() + CLOSE_TIMEOUT;
    let state_text = run_close_cli(
        binary,
        state,
        "quiesce",
        &[("--authority-file", authority)],
        deadline,
    )?;
    if state_text.trim() != "fenced" {
        let signer = generation_dir.join("agent.key");
        let tag = generation_dir.join("owner.auth-tag");
        if signer.exists() && tag.exists() {
            for _ in 0..64 {
                let result = run_close_cli(
                    binary,
                    state,
                    "drain",
                    &[
                        ("--authority-file", authority),
                        ("--signing-key-file", &signer),
                        ("--auth-tag-file", &tag),
                    ],
                    deadline,
                )?;
                if result.trim() == "no pending effect" {
                    break;
                }
                if result.trim() != "drained" {
                    return Err("broker drain returned an unknown result".into());
                }
            }
        }
    }
    run_close_cli(
        binary,
        state,
        "fence",
        &[("--authority-file", authority)],
        deadline,
    )?;
    Ok(())
}

fn run_close_cli(
    binary: &Path,
    state: &Path,
    verb: &str,
    args: &[(&str, &Path)],
    deadline: Instant,
) -> Result<String, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("broker generation close timed out; authority remains quiescing".into());
    }
    run_cli_with_timeout(binary, state, verb, args, remaining.min(CLI_TIMEOUT))
}

fn run_cli(
    binary: &Path,
    state: &Path,
    verb: &str,
    args: &[(&str, &Path)],
) -> Result<String, String> {
    run_cli_with_timeout(binary, state, verb, args, CLI_TIMEOUT)
}

fn run_cli_with_timeout(
    binary: &Path,
    state: &Path,
    verb: &str,
    args: &[(&str, &Path)],
    timeout: Duration,
) -> Result<String, String> {
    let mut command = Command::new(binary);
    command.arg("--state").arg(state).arg(verb);
    for (name, value) in args {
        command.arg(name).arg(value);
    }
    let mut child = command
        .env_remove("BUZZ_PRIVATE_KEY")
        .env_remove("NOSTR_PRIVATE_KEY")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| format!("failed to start broker {verb}"))?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                let output = child
                    .wait_with_output()
                    .map_err(|_| format!("failed to collect broker {verb}"))?;
                if !output.status.success() {
                    return Err(format!("broker {verb} failed ({})", output.status));
                }
                return String::from_utf8(output.stdout)
                    .map_err(|_| format!("broker {verb} returned invalid output"));
            }
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!("broker {verb} timed out or could not be inspected"));
            }
        }
    }
}

fn ensure_private_dir(path: &Path) -> Result<(), String> {
    if !path.exists() {
        std::fs::create_dir(path).map_err(|_| "cannot create broker directory")?;
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| "cannot inspect broker directory")?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("broker directory is not a real directory".into());
    }
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| "cannot secure broker directory".into())
}

fn write_private_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| "cannot create broker private file")?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| "cannot persist broker private file".into())
}

fn write_current(current: &Path, bytes: &[u8]) -> Result<(), String> {
    let temporary = current.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    write_private_new(&temporary, bytes)?;
    if let Err(error) = std::fs::rename(&temporary, current) {
        let _ = std::fs::remove_file(&temporary);
        return Err(format!("cannot persist broker current authority: {error}"));
    }
    let parent = current
        .parent()
        .ok_or("missing broker authority directory")?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| "cannot sync broker authority directory".into())
}

/// One exact pubkey may opt into the local broker canary. A malformed selector
/// fails closed instead of silently launching a selected agent with its nsec.
pub(super) fn selected_agent(selector: Option<&str>, pubkey: &str) -> Result<bool, String> {
    let Some(selector) = selector else {
        return Ok(false);
    };
    if selector.len() != 64 || !selector.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY must be one 64-hex agent pubkey".into());
    }
    Ok(selector.eq_ignore_ascii_case(pubkey))
}

/// Broker ACP currently needs an explicit channel set and owner-only gate.
/// Validate before issuing a bearer so an invalid canary leaves no authority.
pub(super) fn validate_canary_prerequisites(
    acp_command: &str,
    local: bool,
    auth_tag: Option<&str>,
    owner_hex: Option<&str>,
    respond_to: &str,
    env: &BTreeMap<String, String>,
) -> Result<(), String> {
    let is_buzz_acp = std::path::Path::new(acp_command)
        .file_name()
        .is_some_and(|name| name == "buzz-acp");
    if !is_buzz_acp || !local {
        return Err("broker canary requires a local buzz-acp runtime".into());
    }
    if auth_tag.is_none_or(|tag| tag.trim().is_empty()) {
        return Err("broker canary requires a signed NIP-OA owner attestation".into());
    }
    if owner_hex.is_none_or(|owner| {
        owner.len() != 64 || !owner.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err("broker canary requires the exact workspace owner pubkey".into());
    }
    if respond_to != "owner-only" {
        return Err("broker canary currently requires owner-only access".into());
    }
    let Some(channels) = env.get("BUZZ_ACP_CHANNELS") else {
        return Err("broker canary requires saved BUZZ_ACP_CHANNELS".into());
    };
    if channels.is_empty()
        || channels
            .split(',')
            .any(|channel| uuid::Uuid::parse_str(channel).is_err())
    {
        return Err("broker canary requires comma-separated channel UUIDs".into());
    }
    Ok(())
}

/// The selected ACP process gets a single opaque bearer; all inherited local
/// signer paths are explicitly removed, including legacy aliases and git auth.
pub(super) fn apply_broker_child_env(
    command: &mut Command,
    broker_url: &str,
    credential: &str,
    relay_url: &str,
    authority_json: &str,
) {
    for key in [
        "BUZZ_PRIVATE_KEY",
        "NOSTR_PRIVATE_KEY",
        "BUZZ_ACP_PRIVATE_KEY",
        "BUZZ_AUTH_TAG",
        "BUZZ_API_TOKEN",
        "BUZZ_ACP_API_TOKEN",
        "BUZZ_RELAY_URL",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_CONFIG_KEY_1",
        "GIT_CONFIG_VALUE_1",
    ] {
        command.env_remove(key);
    }
    command
        .env("BUZZ_AGENT_MODE", "broker")
        .env("BUZZ_BROKER_URL", broker_url)
        .env("BUZZ_BROKER_CREDENTIAL", credential)
        .env("BUZZ_BROKER_RELAY_URL", relay_url)
        .env("BUZZ_MANAGED_ACP_AUTHORITY", authority_json);
}

#[cfg(all(test, unix))]
#[path = "broker/local_media_proof.rs"]
mod local_media_proof;
#[cfg(test)]
mod tests;
