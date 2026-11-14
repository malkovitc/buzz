#![cfg(unix)]

use std::io::Read as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn selected_loopback_port_is_private_and_host_exits_when_desktop_pipe_closes() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = directory.path().join("broker.db");
    let output = directory.path().join("listen.txt");
    let mut child = Command::new(env!("CARGO_BIN_EXE_buzz-broker-host"))
        .args([
            "--state",
            state.to_str().unwrap(),
            "serve",
            "--bind",
            "127.0.0.1:0",
            "--listen-output",
            output.to_str().unwrap(),
            "--exit-on-stdin-eof",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !output.exists() && child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if !output.exists() {
        let _ = child.kill();
        let _ = child.wait();
        let mut stderr = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr)
            .unwrap();
        panic!("host did not publish its listener: {stderr}");
    }
    let mode = std::fs::metadata(&output).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let address = std::fs::read_to_string(&output).unwrap();
    let address: std::net::SocketAddr = address.trim().parse().unwrap();
    assert!(address.ip().is_loopback());
    assert_ne!(address.port(), 0);
    std::net::TcpStream::connect_timeout(&address, Duration::from_secs(1)).unwrap();

    drop(child.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(5);
    while child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    let status = child.try_wait().unwrap().unwrap_or_else(|| {
        let _ = child.kill();
        child.wait().unwrap()
    });
    assert!(status.success(), "host did not exit cleanly: {status}");
}

#[test]
fn listener_output_existing_path_is_never_overwritten() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let state = directory.path().join("broker.db");
    let output = directory.path().join("listen.txt");
    std::fs::write(&output, b"sentinel").unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_buzz-broker-host"))
        .args([
            "--state",
            state.to_str().unwrap(),
            "serve",
            "--bind",
            "127.0.0.1:0",
            "--listen-output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(!result.status.success());
    assert_eq!(std::fs::read(&output).unwrap(), b"sentinel");
}
