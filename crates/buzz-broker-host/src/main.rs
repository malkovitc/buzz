mod credential;
mod http;
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
    /// Serve the broker action endpoint on loopback.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: SocketAddr,
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
        Command::Serve {
            bind,
            signing_key_file,
            auth_tag_file,
        } => {
            if !bind.ip().is_loopback() {
                return Err(http::HostError::NonLoopback.into());
            }
            let listener = tokio::net::TcpListener::bind(bind).await?;
            eprintln!("broker authority host listening on loopback");
            #[cfg(unix)]
            match (signing_key_file, auth_tag_file) {
                (Some(key), Some(tag)) => {
                    let signer = signing::HostSigner::load(&key, &tag)?;
                    http::serve_with_effects(listener, store, signer).await?;
                }
                (None, None) => http::serve(listener, store).await?,
                _ => return Err("both signer files are required for host-owned effects".into()),
            }
            #[cfg(not(unix))]
            {
                let _ = (signing_key_file, auth_tag_file);
                http::serve(listener, store).await?;
            }
        }
    }
    Ok(())
}
