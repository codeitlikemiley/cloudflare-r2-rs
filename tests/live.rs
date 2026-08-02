//! End-to-end tests against a real R2 bucket.
//!
//! These are `#[ignore]`d so `cargo test` stays offline. To run them, set
//! `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and `R2_BUCKET`
//! against a **disposable** bucket, then:
//!
//! ```sh
//! cargo test --test live -- --ignored --test-threads=1
//! ```
//!
//! Every test namespaces its keys and cleans up after itself.

use std::time::Duration;

use cloudflare_r2_rs::{ListOptions, MultipartOptions, PutOptions, R2Client, Result};

/// Builds a client from the environment, or explains what is missing.
fn client() -> R2Client {
    let _ = dotenvy::dotenv();
    R2Client::from_env().expect(
        "live tests need R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY and R2_BUCKET",
    )
}

/// A key prefix unique to one test *and* one run.
///
/// A fixed prefix would let a failed run leave objects behind that break every
/// later run's exact-count assertions, so the process ID and a monotonic
/// counter are folded in.
fn prefix(test: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);

    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "cloudflare-r2-rs-tests/{run}-{}-{unique}/{test}/",
        std::process::id()
    )
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn round_trips_an_object() -> Result<()> {
    let client = client();
    let key = format!("{}hello.txt", prefix("round_trip"));

    client
        .put_object_with(
            &key,
            "hello world",
            PutOptions::new()
                .content_type("text/plain")
                .metadata("origin", "live-test"),
        )
        .await?;

    assert!(client.object_exists(&key).await?);

    let body = client.get_object(&key).await?;
    assert_eq!(body, b"hello world");

    let metadata = client.head_object(&key).await?;
    assert_eq!(metadata.content_length, 11);
    assert_eq!(metadata.content_type.as_deref(), Some("text/plain"));
    // Streamed uploads must not leave R2's aws-chunked framing on the object.
    assert_eq!(metadata.content_encoding, None);
    assert_eq!(
        metadata.metadata.get("origin").map(String::as_str),
        Some("live-test")
    );

    let range = client.get_object_range(&key, 0, Some(4)).await?;
    assert_eq!(range, b"hello");

    client.delete_object(&key).await?;
    assert!(!client.object_exists(&key).await?);
    Ok(())
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn reports_missing_objects_as_not_found() -> Result<()> {
    let client = client();
    let err = client
        .get_object(&format!("{}nope.txt", prefix("missing")))
        .await
        .unwrap_err();
    assert!(err.is_not_found(), "expected not-found, got {err}");
    Ok(())
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn lists_copies_and_deletes_by_prefix() -> Result<()> {
    let client = client();
    let base = prefix("listing");

    for index in 0..5 {
        client
            .put_object(&format!("{base}file-{index}.txt"), vec![b'x'; 16])
            .await?;
    }
    client
        .put_object(&format!("{base}nested/deep.txt"), vec![b'y'; 8])
        .await?;

    let all = client.list_all_objects(Some(&base)).await?;
    assert_eq!(all.len(), 6);

    // Paging: one key at a time still reaches every object.
    let page = client
        .list_objects(ListOptions::new().prefix(&base).max_keys(1))
        .await?;
    assert_eq!(page.objects.len(), 1);
    assert!(page.is_truncated);
    assert!(page.next_continuation_token.is_some());

    // Delimiter-based browsing surfaces the nested "folder".
    let prefixes = client.list_prefixes(&base).await?;
    assert!(prefixes.iter().any(|p| p.ends_with("nested/")));

    client
        .copy_object(&format!("{base}file-0.txt"), &format!("{base}copy.txt"))
        .await?;
    assert!(client.object_exists(&format!("{base}copy.txt")).await?);

    let report = client.delete_prefix(&base).await?;
    assert!(report.all_succeeded(), "failures: {:?}", report.failed);
    assert!(client.list_all_objects(Some(&base)).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn uploads_and_downloads_files() -> Result<()> {
    let client = client();
    let base = prefix("files");
    let key = format!("{base}payload.bin");

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("payload.bin");
    tokio::fs::write(&source, vec![b'z'; 128 * 1024]).await?;

    client.upload_file(&key, &source).await?;

    // The upload streams from disk; that path must not pick up aws-chunked
    // content-encoding, which R2 stores rather than strips.
    assert_eq!(client.head_object(&key).await?.content_encoding, None);

    let destination = directory.path().join("downloaded.bin");
    let written = client.download_to(&key, &destination).await?;
    assert_eq!(written, 128 * 1024);
    assert_eq!(tokio::fs::read(&destination).await?.len(), 128 * 1024);

    client.delete_prefix(&base).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn uploads_a_large_file_via_multipart() -> Result<()> {
    let client = client();
    let base = prefix("multipart");
    let key = format!("{base}large.bin");

    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("large.bin");
    // Two 5 MiB parts plus a remainder, the smallest genuinely multipart case.
    let size = 11 * 1024 * 1024;
    tokio::fs::write(&source, vec![b'm'; size]).await?;

    client
        .multipart_upload_file(
            &key,
            &source,
            MultipartOptions::new()
                .part_size(5 * 1024 * 1024)
                .concurrency(3),
        )
        .await?;

    let metadata = client.head_object(&key).await?;
    assert_eq!(metadata.content_length, size as i64);

    client.delete_prefix(&base).await?;
    Ok(())
}

/// Proves R2 actually accepts the signature, which asserting on the URL string
/// cannot: the URL is fetched with a plain HTTP client and the body compared.
///
/// Uses `curl` rather than adding an HTTP client dependency for one test; the
/// test is skipped if curl is unavailable.
#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn a_presigned_url_is_accepted_by_r2() -> Result<()> {
    let client = client();
    let base = prefix("presigned");
    let key = format!("{base}signed.txt");

    client.put_object(&key, "signed body").await?;

    let url = client
        .presign_get(&key, Duration::from_secs(120))
        .await?
        .into_url();

    match std::process::Command::new("curl")
        .args(["--silent", "--show-error", "--fail", &url])
        .output()
    {
        Ok(output) if output.status.success() => {
            assert_eq!(String::from_utf8_lossy(&output.stdout), "signed body");
        }
        Ok(output) => panic!(
            "R2 rejected the presigned URL: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipping presigned fetch: curl not available");
        }
        Err(err) => panic!("could not run curl: {err}"),
    }

    client.delete_prefix(&base).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires live R2 credentials"]
async fn bucket_existence_is_reported_accurately() -> Result<()> {
    let client = client();
    assert!(client.bucket_exists().await?);

    // A bucket-scoped API token gets 403 rather than 404 for a foreign bucket,
    // and this crate deliberately surfaces that as an error rather than
    // pretending the bucket does not exist. Either outcome is correct; what
    // must not happen is a silent `true`.
    match client
        .with_bucket("cloudflare-r2-rs-definitely-not-a-bucket")
        .bucket_exists()
        .await
    {
        Ok(false) => {}
        Ok(true) => panic!("a nonexistent bucket was reported as existing"),
        Err(err) => assert_eq!(
            err.status(),
            Some(403),
            "expected 404 -> false or a 403 error, got {err}"
        ),
    }
    Ok(())
}
