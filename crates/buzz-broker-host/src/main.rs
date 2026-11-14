mod credential;
#[cfg(unix)]
mod drain;
mod http;
#[cfg(unix)]
mod huddle;
mod issuer;
#[cfg(unix)]
mod publisher;
#[cfg(unix)]
mod signing;
mod store;

#[cfg(all(test, unix))]
mod tests;

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use store::AuthorityStore;

#[derive(Parser)]
#[command(about = "Durable managed-ACP authority broker host")]
struct Cli {
    /// Absolute path to the broker SQLite state in an owner-only directory.
    #[arg(long)]
    state: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect the durable state of one exact authority without changing it.
    Inspect {
        #[arg(long)]
        authority_file: PathBuf,
    },
    /// Issue one exact active authority and write its bearer to a new mode-0600 file.
    Issue {
        /// JSON file containing one strict AuthorityIdentity.
        #[arg(long)]
        authority_file: PathBuf,
        /// New file that receives the opaque bearer. Existing paths are never overwritten.
        #[arg(long)]
        credential_output: PathBuf,
    },
    /// Monotonically fence one exact previously issued authority.
    Fence {
        /// JSON file containing the exact issued AuthorityIdentity.
        #[arg(long)]
        authority_file: PathBuf,
    },
    /// Stop new work for one exact authority while existing effects drain.
    Quiesce {
        /// JSON file containing the exact issued AuthorityIdentity.
        #[arg(long)]
        authority_file: PathBuf,
    },
    /// Reconcile one previously admitted message effect after quiesce.
    Drain {
        #[arg(long)]
        authority_file: PathBuf,
        #[arg(long)]
        signing_key_file: PathBuf,
        #[arg(long)]
        auth_tag_file: PathBuf,
    },
    /// Serve the broker action endpoint on loopback.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
        /// New owner-only file receiving the actual loopback address (supports port 0).
        #[arg(long)]
        listen_output: Option<PathBuf>,
        /// Stop serving when the launching Desktop closes its stdin pipe.
        #[arg(long)]
        exit_on_stdin_eof: bool,
        /// Owner-only file containing the agent signing key for host-owned effects.
        #[arg(long)]
        signing_key_file: Option<PathBuf>,
        /// Owner-only file containing the signed NIP-OA owner attestation.
        #[arg(long)]
        auth_tag_file: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let store = AuthorityStore::open(&cli.state).await?;
    match cli.command {
        Command::Inspect { authority_file } => {
            let identity = issuer::read_identity(&authority_file)?;
            match store.inspect(&identity).await? {
                None => println!("missing"),
                Some(buzz_sdk::broker::AuthorityState::Active) => println!("active"),
                Some(buzz_sdk::broker::AuthorityState::Quiescing) => println!("quiescing"),
                Some(buzz_sdk::broker::AuthorityState::Fenced) => println!("fenced"),
            }
        }
        Command::Issue {
            authority_file,
            credential_output,
        } => {
            issuer::issue_to_file(&store, &authority_file, &credential_output).await?;
            println!("issued");
        }
        Command::Fence { authority_file } => {
            issuer::fence_from_file(&store, &authority_file).await?;
            println!("fenced");
        }
        Command::Quiesce { authority_file } => {
            let state = issuer::quiesce_from_file(&store, &authority_file).await?;
            match state {
                buzz_sdk::broker::AuthorityState::Quiescing => println!("quiescing"),
                buzz_sdk::broker::AuthorityState::Fenced => println!("fenced"),
                buzz_sdk::broker::AuthorityState::Active => unreachable!("quiesce closes intake"),
            }
        }
        Command::Drain {
            authority_file,
            signing_key_file,
            auth_tag_file,
        } => {
            #[cfg(unix)]
            {
                let identity = issuer::read_identity(&authority_file)?;
                let signer = signing::HostSigner::load(&signing_key_file, &auth_tag_file)?;
                let publisher = publisher::RelayPublisher::new()?;
                let drained = drain::drain_pending(&store, &identity, &signer, &publisher).await?;
                println!(
                    "{}",
                    if drained {
                        "drained"
                    } else {
                        "no pending effect"
                    }
                );
            }
            #[cfg(not(unix))]
            {
                let _ = (authority_file, signing_key_file, auth_tag_file);
                return Err("effect drain is unsupported on this platform".into());
            }
        }
        Command::Serve {
            bind,
            listen_output,
            exit_on_stdin_eof,
            signing_key_file,
            auth_tag_file,
        } => {
            if !bind.ip().is_loopback() {
                return Err(http::HostError::NonLoopback.into());
            }
            #[cfg(unix)]
            let signer = match (signing_key_file, auth_tag_file) {
                (Some(key), Some(tag)) => Some(signing::HostSigner::load(&key, &tag)?),
                (None, None) => None,
                _ => return Err("both signer files are required for host-owned effects".into()),
            };
            let listener = tokio::net::TcpListener::bind(bind).await?;
            if let Some(path) = listen_output.as_deref() {
                write_listen_output(path, listener.local_addr()?)?;
            }
            eprintln!("broker authority host listening on loopback");
            let service = async move {
                #[cfg(unix)]
                match signer {
                    Some(signer) => http::serve_with_effects(listener, store, signer).await?,
                    None => http::serve(listener, store).await?,
                }
                #[cfg(not(unix))]
                {
                    let _ = (signing_key_file, auth_tag_file);
                    http::serve(listener, store).await?;
                }
                Ok::<(), Box<dyn std::error::Error>>(())
            };
            if exit_on_stdin_eof {
                tokio::select! {
                    result = service => result?,
                    result = wait_for_stdin_eof() => result?,
                }
            } else {
                service.await?;
            }
        }
    }
    Ok(())
}

async fn wait_for_stdin_eof() -> std::io::Result<()> {
    use tokio::io::AsyncReadExt as _;

    let mut byte = [0_u8; 1];
    let received = tokio::io::stdin().read(&mut byte).await?;
    if received == 0 {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "broker parent pipe must remain empty",
        ))
    }
}

#[cfg(unix)]
fn write_listen_output(path: &std::path::Path, address: SocketAddr) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

    let parent = path
        .parent()
        .ok_or_else(|| std::io::Error::other("missing output directory"))?;
    let metadata = std::fs::symlink_metadata(parent)?;
    if !path.is_absolute()
        || !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(std::io::Error::other("insecure listener output directory"));
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    writeln!(file, "{address}")?;
    file.sync_all()
}

#[cfg(not(unix))]
fn write_listen_output(_path: &std::path::Path, _address: SocketAddr) -> std::io::Result<()> {
    Err(std::io::Error::other(
        "listener output is unsupported on this platform",
    ))
}
