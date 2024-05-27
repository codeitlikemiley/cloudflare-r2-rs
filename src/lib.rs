use std::sync::Arc;
use aws_config::SdkConfig;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use log::{debug, error, info};

// Define states for the S3 operations
pub enum S3State {
    NotInitialized,
    Initialized(SdkConfig),
}

// Define a builder for the S3Manager
pub struct R2ManagerBuilder {
    bucket_name: Option<String>,
    url: Option<String>,
    client_id: Option<String>,
    secret_key: Option<String>,
}

impl R2ManagerBuilder {
    pub fn new() -> Self {
        R2ManagerBuilder {
            bucket_name: None,
            url: None,
            client_id: None,
            secret_key: None,
        }
    }

    pub fn bucket_name(mut self, bucket_name: &str) -> Self {
        self.bucket_name = Some(bucket_name.to_string());
        self
    }

    pub fn url(mut self, uri: &str) -> Self {
        self.url = Some(uri.to_string());
        self
    }

    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }

    pub fn secret_key(mut self, secret: &str) -> Self {
        self.secret_key = Some(secret.to_string());
        self
    }

    pub async fn build(self) -> Result<CloudFlareR2, Box<dyn std::error::Error>> {
        let bucket_name = self.bucket_name.ok_or("Bucket name is required")?;
        let url = self.url.ok_or("Cloudflare KV URI is required")?;
        let client_id = self.client_id.ok_or("Cloudflare KV client ID is required")?;
        let secret_key = self.secret_key.ok_or("Cloudflare KV secret is required")?;

        std::env::set_var("AWS_ACCESS_KEY_ID", &client_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &secret_key);

        let s3_config = aws_config::load_from_env()
            .await
            .into_builder()
            .endpoint_url(&url)
            .region(Region::new("us-east-1"))
            .build();

        Ok(CloudFlareR2 {
            bucket_name,
            client: Arc::new(Client::new(&s3_config)),
            state: S3State::Initialized(s3_config),
        })
    }
}

// Define the S3Manager struct
pub struct CloudFlareR2 {
    bucket_name: String,
    client: Arc<Client>,
    state: S3State,
}

impl CloudFlareR2 {
    pub fn builder() -> R2ManagerBuilder {
        R2ManagerBuilder::new()
    }

    pub fn get_bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub async fn create_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.state {
            S3State::Initialized(_) => {
                let create_bucket_request = self.client
                    .create_bucket()
                    .bucket(&self.bucket_name);

                let result = create_bucket_request.send().await;

                if result.is_ok() {
                    debug!("{:?}", result.unwrap());
                    info!("Created successfully {}", self.bucket_name);
                    Ok(())
                } else {
                    debug!("{:?}", result.unwrap_err());
                    error!("Creation of {} failed.", self.bucket_name);
                    Err("Failed to create bucket".into())
                }
            }
            _ => {
                error!("S3 client not initialized");
                Err("S3 client not initialized".into())
            }
        }
    }

    pub async fn delete_bucket(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.state {
            S3State::Initialized(_) => {
                let delete_bucket_request = self.client
                    .delete_bucket()
                    .bucket(&self.bucket_name);

                let result = delete_bucket_request.send().await;

                if result.is_ok() {
                    debug!("{:?}", result.unwrap());
                    info!("Deleted successfully {}", self.bucket_name);
                    Ok(())
                } else {
                    debug!("{:?}", result.unwrap_err());
                    error!("Deletion of {} failed.", self.bucket_name);
                    Err("Failed to delete bucket".into())
                }
            }
            _ => {
                error!("S3 client not initialized");
                Err("S3 client not initialized".into())
            }
        }
    }

    pub async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        match &self.state {
            S3State::Initialized(_) => {
                let put_object_request = self.client
                    .put_object()
                    .bucket(&self.bucket_name)
                    .key(key)
                    .body(ByteStream::from(body));

                let result = put_object_request.send().await;

                if result.is_ok() {
                    debug!("{:?}", result.unwrap());
                    info!("Put object successfully {}", key);
                    Ok(())
                } else {
                    debug!("{:?}", result.unwrap_err());
                    error!("Put object {} failed.", key);
                    Err("Failed to put object".into())
                }
            }
            _ => {
                error!("S3 client not initialized");
                Err("S3 client not initialized".into())
            }
        }
    }

    // delete object
    pub async fn delete_object(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        match &self.state {
            S3State::Initialized(_) => {
                let delete_object_request = self.client
                    .delete_object()
                    .bucket(&self.bucket_name)
                    .key(key);

                let result = delete_object_request.send().await;

                match result {
                    Ok(_) => {
                        info!("Deleted object successfully {}", key);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to delete object {}", key);
                        Err(e.into())
                    }
                }
            }
            _ => {
                error!("S3 client not initialized");
                Err("S3 client not initialized".into())
            }
        }
    }

    // get object
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match &self.state {
            S3State::Initialized(_) => {
                let get_object_request = self.client
                    .get_object()
                    .bucket(&self.bucket_name)
                    .key(key);

                let result = get_object_request.send().await;
                //result.unwrap().body.collect().await.unwrap().into_bytes().to_vec()
                match result {
                    Ok(response) => {
                        let body = response.body.collect().await.unwrap().into_bytes().to_vec();
                        info!("Got object successfully {}", key);
                        Ok(body)
                    }
                    Err(e) => {
                        error!("Failed to get object {}", key);
                        Err(e.into())
                    }
                }
            }
            _ => {
                error!("S3 client not initialized");
                Err("S3 client not initialized".into())
            }
        }
    }

}

