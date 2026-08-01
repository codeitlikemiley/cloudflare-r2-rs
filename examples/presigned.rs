//! Handing out time-limited URLs so clients can transfer objects directly.
//!
//! ```sh
//! cargo run --example presigned
//! ```

use std::time::Duration;

use cloudflare_r2_rs::{PresignOptions, R2Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let client = R2Client::from_env()?;

    // A URL a browser can upload straight to, no credentials involved.
    let upload = client
        .presign_put_with(
            "examples/user-upload.png",
            PresignOptions::new()
                .expires_in(Duration::from_secs(15 * 60))
                .content_type("image/png"),
        )
        .await?;
    println!("PUT {}", upload.url);
    for (name, value) in &upload.headers {
        // These were signed, so the client must send them verbatim.
        println!("  header {name}: {value}");
    }

    // A download URL that makes the browser save the file under a nicer name.
    let download = client
        .presign_get_with(
            "examples/user-upload.png",
            PresignOptions::new()
                .expires_in(Duration::from_secs(60))
                .response_content_disposition("attachment; filename=\"avatar.png\""),
        )
        .await?;
    println!("GET {}", download.url);

    // And one that lets a client delete the object.
    let delete = client
        .presign_delete("examples/user-upload.png", Duration::from_secs(60))
        .await?;
    println!("DELETE {}", delete.url);

    Ok(())
}
