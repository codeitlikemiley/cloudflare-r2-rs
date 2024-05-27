#![allow(unused)]

use anyhow::{anyhow, Result};
use cloudflare_r2_rs::CloudFlareR2;
use std::path::{Path, PathBuf};
use tokio::fs::read;
// allow unused imports
#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let secret_key =
        std::env::var("CLOUDFLARE_SECRET_KEY").expect("CLOUDFLARE_SECRET_KEY must be set");
    let client_id =
        std::env::var("CLOUDFLARE_CLIENT_ID").expect("CLOUDFLARE_CLIENT_ID must be set");
    let bucket_name =
        std::env::var("CLOUDFLARE_BUCKET_NAME").expect("CLOUDFLARE_BUCKET_NAME must be set");
    let url = std::env::var("CLOUDFLARE_URL").expect("CLOUDFLARE_URL must be set");

    let manager = CloudFlareR2::builder()
        // set all the required fields , this will have compile time error if not set
        .bucket_name(&bucket_name)
        .secret_key(&secret_key)
        .client_id(&client_id)
        .url(&url)
        // build the manager
        .build()?;

    let keys = &manager.list_keys().await;
    eprintln!("keys = {:#?}", keys);

    let uploaded_file = manager
        .put_object("Cargo.lock", read("Cargo.lock").await?)
        .await?;
    eprintln!("uploaded file = {:#?}", uploaded_file);

    let data = manager.get_object("Cargo.lock").await?;

    let download_path = manager
        .download_file(
            "Screenshot 2024-05-27 at 11.18.41 PM.png",
            PathBuf::from("src/").as_path(),
        )
        .await?;
    eprintln!("download_path = {:#?}", download_path);

    let deleted = manager.delete_object("Cargo.lock").await?;
    eprintln!("deleted = {:#?}", deleted);

    Ok(())
}
