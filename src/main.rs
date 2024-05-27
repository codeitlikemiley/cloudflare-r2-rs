#![allow(unused)]

use std::path::{Path, PathBuf};
use tokio::fs::read;
use cloudflare_r2_rs::CloudFlareR2;
use anyhow::{anyhow, Result};
// allow unused imports
#[tokio::main]
async fn main() -> Result<()> {
    // First Set up the environment variables
    dotenv::dotenv().ok();
    let secret_key = std::env::var("CLOUDFLARE_SECRET_KEY").expect("CLOUDFLARE_SECRET_KEY must be set");
    let client_id = std::env::var("CLOUDFLARE_CLIENT_ID").expect("CLOUDFLARE_CLIENT_ID must be set");
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME").expect("CLOUDFLARE_BUCKET_NAME must be set");
    let url = std::env::var("CLOUDFLARE_URL").expect("CLOUDFLARE_URL must be set");

    // Initialize the CloudFlareR2 manager with the builder
    let manager = CloudFlareR2::builder()
        // set all the required fields
        .bucket_name(&bucket_name)
        .secret_key(&secret_key)
        .client_id(&client_id)
        .url(&url)
        // build the manager
        .build()?;

    // List all the keys in the bucket
    let keys = &manager.list_keys().await;
    eprintln!("keys = {:#?}", keys);

    // Upload a file to the bucket
    let uploaded_file = manager.put_object("Cargo.lock", read("Cargo.lock").await?).await?;
    eprintln!("uploaded file = {:#?}", uploaded_file);

    let data = manager.get_object("Cargo.lock").await?;
    // eprintln!("data = {:#?}", data);

    // Download the file from the bucket
    let download_path =  manager.download_file("Screenshot 2024-05-27 at 11.18.41 PM.png", PathBuf::from("src/").as_path()).await?;
    eprintln!("download_path = {:#?}", download_path);

    // Delete the file from the bucket
    let deleted = manager.delete_object("Cargo.lock").await?;
    eprintln!("deleted = {:#?}", deleted);

    Ok(())
}



