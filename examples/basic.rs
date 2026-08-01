//! Everyday object operations against a real bucket.
//!
//! Set `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and
//! `R2_BUCKET` (a `.env` file works), then:
//!
//! ```sh
//! cargo run --example basic
//! ```

use cloudflare_r2_rs::{ListOptions, PutOptions, R2Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let client = R2Client::from_env()?;
    println!("bucket {} at {}", client.bucket(), client.endpoint());

    // Store an object with explicit caching and user metadata.
    let outcome = client
        .put_object_with(
            "examples/greeting.txt",
            b"hello from cloudflare-r2-rs".to_vec(),
            PutOptions::new()
                .content_type("text/plain; charset=utf-8")
                .cache_control("public, max-age=300")
                .metadata("source", "basic-example"),
        )
        .await?;
    println!("put {} (etag {:?})", outcome.key, outcome.etag);

    // Read it back.
    let body = client.get_object("examples/greeting.txt").await?;
    println!(
        "read {} bytes: {}",
        body.len(),
        String::from_utf8_lossy(&body)
    );

    // Only the first eleven bytes.
    let head = client
        .get_object_range("examples/greeting.txt", 0, Some(10))
        .await?;
    println!("range: {}", String::from_utf8_lossy(&head));

    // Metadata without transferring the body.
    let metadata = client.head_object("examples/greeting.txt").await?;
    println!(
        "content-type {:?}, {} bytes, metadata {:?}",
        metadata.content_type, metadata.content_length, metadata.metadata
    );

    // Missing keys are a typed error, not a surprise.
    match client.get_object("examples/definitely-missing.txt").await {
        Ok(_) => println!("unexpectedly found it"),
        Err(err) if err.is_not_found() => println!("missing key reported as not-found"),
        Err(err) => return Err(err),
    }

    // Copy, then list the prefix one page at a time.
    client
        .copy_object("examples/greeting.txt", "examples/greeting-copy.txt")
        .await?;

    let page = client
        .list_objects(ListOptions::new().prefix("examples/").max_keys(50))
        .await?;
    for object in &page.objects {
        println!("  {} ({} bytes)", object.key, object.size);
    }

    // Clean up everything this example created.
    let report = client.delete_prefix("examples/").await?;
    println!(
        "deleted {} objects, {} failures",
        report.deleted.len(),
        report.failed.len()
    );

    Ok(())
}
