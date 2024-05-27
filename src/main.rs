use tokio::fs::read;
use cloudflare_r2_rs::CloudFlareR2;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let secret_key = std::env::var("CLOUDFLARE_SECRET_KEY").expect("CLOUDFLARE_SECRET_KEY must be set");
    let client_id = std::env::var("CLOUDFLARE_CLIENT_ID").expect("CLOUDFLARE_CLIENT_ID must be set");
    let bucket_name = std::env::var("CLOUDFLARE_BUCKET_NAME").expect("CLOUDFLARE_BUCKET_NAME must be set");
    let url = std::env::var("CLOUDFLARE_URL").expect("CLOUDFLARE_URL must be set");

    let manager = CloudFlareR2::builder()
        .bucket_name(&bucket_name)
        .url(&url)
        .client_id(&client_id)
        .secret_key(&secret_key)
        .build()
        .unwrap();

    // let _ = put_object(&manager, "./Cargo.toml", "Cargo.toml").await;


    // let key = "Screenshot 2024-05-27 at 11.18.41 PM.png";
    // let dir = std::path::Path::new("src/");
    // let result =  manager.download_file(key, dir).await;
    // println!("{:?}", result);


    let result = manager.list_keys().await;
    println!("{:?}", result);
    // let _ = get_object(&manager, "Cargo.toml").await;
    // let _ = delete_object(&manager, "Cargo.toml").await;

}



async fn put_object(manager:  &CloudFlareR2, path: &str, key: &str) {
    let path = std::path::PathBuf::from(path);
    let data = read(path).await.unwrap();
    let result = manager.put_object(key, data).await;

    if let Err(e) = result {
        println!("{:?}", e);
    } else {
        println!("Object uploaded successfully");
    }
}

async fn delete_object(manager: &CloudFlareR2, key: &str) {
    let result = manager.delete_object(key).await;

    if let Err(e) = result {
        println!("{:?}", e);
    } else {
        println!("Object deleted successfully");
    }
}

async fn get_object(manager: &CloudFlareR2, key: &str) {
    let result = manager.get_object(key).await;

    if let Err(e) = result {
        println!("{:?}", e);
    } else {
        let result = String::from_utf8(result.unwrap()).unwrap();
        println!("{:?}", result);
    }
}
