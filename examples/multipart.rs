//! Uploading a large file with concurrent multipart parts.
//!
//! ```sh
//! cargo run --example multipart
//! ```

use std::path::PathBuf;

use cloudflare_r2_rs::{MultipartOptions, PutOptions, R2Client, Result};
use tokio::io::AsyncWriteExt;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let client = R2Client::from_env()?;

    // Build a 40 MiB scratch file so the upload actually goes multipart.
    let path = PathBuf::from("target/multipart-example.bin");
    write_sample_file(&path, 40 * 1024 * 1024).await?;

    let options = MultipartOptions::new()
        .part_size(8 * 1024 * 1024)
        .concurrency(4)
        .put_options(PutOptions::new().content_type("application/octet-stream"));

    let outcome = client
        .upload_file_with("examples/large.bin", &path, options)
        .await?;
    println!("uploaded {} (etag {:?})", outcome.key, outcome.etag);

    let metadata = client.head_object("examples/large.bin").await?;
    println!("stored size: {} bytes", metadata.content_length);

    // Interrupted uploads keep holding storage until they are aborted.
    for upload in client.list_multipart_uploads().await? {
        println!("dangling upload {} for {}", upload.upload_id, upload.key);
        client
            .abort_multipart_upload(&upload.key, &upload.upload_id)
            .await?;
    }

    client.delete_object("examples/large.bin").await?;
    tokio::fs::remove_file(&path).await?;
    Ok(())
}

async fn write_sample_file(path: &std::path::Path, size: usize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = tokio::fs::File::create(path).await?;
    let chunk = vec![b'r'; 1024 * 1024];
    let mut written = 0;
    while written < size {
        let take = chunk.len().min(size - written);
        file.write_all(&chunk[..take]).await?;
        written += take;
    }
    file.flush().await?;
    Ok(())
}
