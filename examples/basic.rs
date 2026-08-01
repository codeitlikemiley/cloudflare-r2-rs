//! Everyday object operations against a real bucket.
//!
//! Set `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and
//! `R2_BUCKET` (a `.env` file works), then:
//!
//! ```sh
//! cargo run --example basic
//! ```
//!
//! Everything this example writes goes under a prefix unique to this run, and
//! it deletes exactly the keys it created. It will not touch anything else in
//! your bucket.

use std::time::{SystemTime, UNIX_EPOCH};

use cloudflare_r2_rs::{ListOptions, PutOptions, R2Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let client = R2Client::from_env()?;
    println!("bucket {} at {}", client.bucket(), client.endpoint());

    // A prefix nothing else can be using, so cleanup is unambiguous.
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or_default();
    let prefix = format!("cloudflare-r2-rs-example/{run}/");
    println!("working under {prefix}");

    let greeting = format!("{prefix}greeting.txt");

    // Store an object with explicit caching and user metadata. The body can be
    // a &str, String, Vec<u8>, byte slice, or a ByteStream.
    let outcome = client
        .put_object_with(
            &greeting,
            "hello from cloudflare-r2-rs",
            PutOptions::new()
                .content_type("text/plain; charset=utf-8")
                .cache_control("public, max-age=300")
                .metadata("source", "basic-example"),
        )
        .await?;
    println!("put {} (etag {:?})", outcome.key, outcome.etag);

    // Read it back.
    let body = client.get_object(&greeting).await?;
    println!(
        "read {} bytes: {}",
        body.len(),
        String::from_utf8_lossy(&body)
    );

    // Only the first eleven bytes.
    let head = client.get_object_range(&greeting, 0, Some(10)).await?;
    println!("range: {}", String::from_utf8_lossy(&head));

    // Metadata without transferring the body.
    let metadata = client.head_object(&greeting).await?;
    println!(
        "content-type {:?}, {} bytes, metadata {:?}",
        metadata.content_type, metadata.content_length, metadata.metadata
    );

    // Missing keys are a typed error, not a surprise.
    match client
        .get_object(&format!("{prefix}definitely-missing.txt"))
        .await
    {
        Ok(_) => println!("unexpectedly found it"),
        Err(err) if err.is_not_found() => println!("missing key reported as not-found"),
        Err(err) => return Err(err),
    }

    // Copy, then list the prefix one page at a time.
    client
        .copy_object(&greeting, &format!("{prefix}greeting-copy.txt"))
        .await?;

    let page = client
        .list_objects(ListOptions::new().prefix(&prefix).max_keys(50))
        .await?;
    for object in &page.objects {
        println!("  {} ({} bytes)", object.key, object.size);
    }

    // Clean up: this prefix is unique to this run, so nothing else can be
    // caught by it.
    let report = client.delete_prefix(&prefix).await?;
    println!(
        "deleted {} objects, {} failures",
        report.deleted.len(),
        report.failed.len()
    );

    Ok(())
}
