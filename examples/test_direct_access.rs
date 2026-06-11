// Manual e2e test for HDLFS direct-access opt-in.
//
// Usage:
//   HDLFS_ENDPOINT=<host> HDLFS_CONTAINER_ID=<id> \
//     HDLFS_CERT=<path> HDLFS_KEY=<path> [HDLFS_PREFIX=<dir>] \
//     cargo run --example test_direct_access \
//       --features hdlfs --no-default-features
//
// All four HDLFS_* vars are required; HDLFS_PREFIX defaults to
// `tmp/test-direct-access/folder-a`.

use std::env;
use std::time::Instant;

use bytes::Bytes;
use object_store::hdlfs::{SAPHdlfsBuilder, SAPHdlfsCredential};
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

const DEFAULT_PREFIX: &str = "tmp/test-direct-access/folder-a";

fn require_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    env::var(name).map_err(|_| format!("missing required env var: {name}").into())
}

fn build(direct_access: bool) -> Result<impl ObjectStore, Box<dyn std::error::Error>> {
    let endpoint = require_env("HDLFS_ENDPOINT")?;
    let container = require_env("HDLFS_CONTAINER_ID")?;
    let cert = require_env("HDLFS_CERT")?;
    let key = require_env("HDLFS_KEY")?;

    Ok(SAPHdlfsBuilder::new()
        .with_endpoint(endpoint)
        .with_container_id(container)
        .with_credential(SAPHdlfsCredential::new(cert, key))
        .with_direct_access(direct_access)
        .with_trace(env::var("HDLFS_TRACE").is_ok())
        .build()?)
}

async fn run(label: &str, direct_access: bool) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n========= {label} (direct_access={direct_access}) =========");
    let store = build(direct_access)?;
    let prefix = env::var("HDLFS_PREFIX").unwrap_or_else(|_| DEFAULT_PREFIX.to_string());
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let key = Path::from(format!(
        "{}/probe-{}.bin",
        prefix.trim_end_matches('/'),
        suffix
    ));

    let payload_bytes = (0..2048u32)
        .flat_map(|i| (i as u8).to_le_bytes())
        .collect::<Vec<u8>>();
    let payload_len = payload_bytes.len();

    let t = Instant::now();
    store
        .put_opts(
            &key,
            PutPayload::from(Bytes::from(payload_bytes.clone())),
            PutOptions {
                mode: PutMode::Overwrite,
                ..Default::default()
            },
        )
        .await?;
    println!("  PUT  ok  {:>6} bytes  {:?}", payload_len, t.elapsed());

    let t = Instant::now();
    let got = store.get(&key).await?.bytes().await?;
    println!("  GET  ok  {:>6} bytes  {:?}", got.len(), t.elapsed());
    assert_eq!(
        got.as_ref(),
        payload_bytes.as_slice(),
        "round-trip mismatch"
    );

    let t = Instant::now();
    let ranged = store.get_range(&key, 100..356).await?;
    println!(
        "  GET  range[100..356]  {:>6} bytes  {:?}",
        ranged.len(),
        t.elapsed()
    );
    assert_eq!(ranged.as_ref(), &payload_bytes[100..356], "range mismatch");

    let t = Instant::now();
    let head = store.head(&key).await?;
    println!("  HEAD ok  size={}  {:?}", head.size, t.elapsed());
    assert_eq!(head.size as usize, payload_len);

    // Put-if-absent: first attempt creates, second must fail with AlreadyExists.
    let create_key = Path::from(format!(
        "{}/poa-{}.bin",
        prefix.trim_end_matches('/'),
        suffix
    ));
    store
        .put_opts(
            &create_key,
            PutPayload::from(Bytes::from_static(b"first")),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await?;
    println!("  PUT  Create (1st)  ok");

    let second = store
        .put_opts(
            &create_key,
            PutPayload::from(Bytes::from_static(b"second")),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await;
    match second {
        Err(object_store::Error::AlreadyExists { .. }) => {
            println!("  PUT  Create (2nd)  -> AlreadyExists  (correct)");
        }
        Err(e) => {
            println!("  PUT  Create (2nd)  -> UNEXPECTED ERR: {e}");
            return Err(format!("expected AlreadyExists, got: {e}").into());
        }
        Ok(_) if !direct_access => {
            // HDLFS namenode (non-DA path) currently ignores `overwrite=false`
            // and silently allows the second PUT. Note this rather than fail.
            println!("  PUT  Create (2nd)  -> Ok  (HDLFS namenode does not enforce; server-side)");
        }
        Ok(_) => {
            return Err("direct-access PUT-if-absent overwrote existing file".into());
        }
    }

    // Cleanup (best effort).
    let _ = store.delete(&key).await;
    let _ = store.delete(&create_key).await;
    println!("  CLEANUP done");

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut errors = Vec::new();
    if let Err(e) = run("direct_access OFF", false).await {
        errors.push(format!("OFF: {e}"));
    }
    if let Err(e) = run("direct_access ON", true).await {
        errors.push(format!("ON: {e}"));
    }
    println!("\n=========================================");
    if errors.is_empty() {
        println!("ALL TESTS PASSED");
        Ok(())
    } else {
        for e in &errors {
            println!("FAIL: {e}");
        }
        Err(format!("{} failure(s)", errors.len()).into())
    }
}
