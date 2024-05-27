use std::sync::Arc;
use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use mime_guess;
use anyhow::{Result, anyhow, bail};
use aws_sdk_s3::config::Credentials;
use log::{debug, error, info};
use std::marker::PhantomData;

pub struct EmptyUrl;

pub struct WithUrl;

pub struct EmptyBucket;

pub struct WithBucket;

pub struct EmptyClientId;

pub struct WithClientId;

pub struct EmptySecret;

pub struct WithSecret;

pub struct R2ManagerBuilder<UrlState, BucketState, ClientIdState, SecretState> {
    bucket_name: Option<String>,
    url: Option<String>,
    client_id: Option<String>,
    secret_key: Option<String>,
    _url_state: PhantomData<UrlState>,
    _bucket_state: PhantomData<BucketState>,
    _client_id_state: PhantomData<ClientIdState>,
    _secret_state: PhantomData<SecretState>,
}

impl R2ManagerBuilder<EmptyUrl, EmptyBucket, EmptyClientId, EmptySecret> {
    pub fn new() -> Self {
        R2ManagerBuilder {
            bucket_name: None,
            url: None,
            client_id: None,
            secret_key: None,
            _url_state: PhantomData,
            _bucket_state: PhantomData,
            _client_id_state: PhantomData,
            _secret_state: PhantomData,
        }
    }
}


impl<UrlState, BucketState, ClientIdState, SecretState> R2ManagerBuilder<UrlState, BucketState, ClientIdState, SecretState> {
    pub fn url(self, uri: &str) -> R2ManagerBuilder<WithUrl, BucketState, ClientIdState, SecretState> {
        R2ManagerBuilder {
            url: Some(uri.to_string()),
            bucket_name: self.bucket_name,
            client_id: self.client_id,
            secret_key: self.secret_key,
            _url_state: PhantomData,
            _bucket_state: PhantomData,
            _client_id_state: PhantomData,
            _secret_state: PhantomData,
        }
    }
    pub fn bucket_name(self, bucket_name: &str) -> R2ManagerBuilder<UrlState, WithBucket, ClientIdState, SecretState> {
        R2ManagerBuilder {
            bucket_name: Some(bucket_name.to_string()),
            url: self.url,
            client_id: self.client_id,
            secret_key: self.secret_key,
            _url_state: PhantomData,
            _bucket_state: PhantomData,
            _client_id_state: PhantomData,
            _secret_state: PhantomData,
        }
    }
    pub fn client_id(self, client_id: &str) -> R2ManagerBuilder<UrlState, BucketState, WithClientId, SecretState> {
        R2ManagerBuilder {
            client_id: Some(client_id.to_string()),
            bucket_name: self.bucket_name,
            url: self.url,
            secret_key: self.secret_key,
            _url_state: PhantomData,
            _bucket_state: PhantomData,
            _client_id_state: PhantomData,
            _secret_state: PhantomData,
        }
    }
    pub fn secret_key(self, secret: &str) -> R2ManagerBuilder<UrlState, BucketState, ClientIdState, WithSecret> {
        R2ManagerBuilder {
            secret_key: Some(secret.to_string()),
            bucket_name: self.bucket_name,
            url: self.url,
            client_id: self.client_id,
            _url_state: PhantomData,
            _bucket_state: PhantomData,
            _client_id_state: PhantomData,
            _secret_state: PhantomData,
        }
    }
}

impl R2ManagerBuilder<WithUrl, WithBucket, WithClientId, WithSecret> {
    pub fn build(self) -> Result<CloudFlareR2> {
        let bucket_name = self.bucket_name.ok_or_else(|| anyhow!("Bucket name is required"))?;
        let url = self.url.ok_or_else(|| anyhow!("Cloudflare URL is required"))?;
        let client_id = self.client_id.ok_or_else(|| anyhow!("Cloudflare R2 client ID is required"))?;
        let secret_key = self.secret_key.ok_or_else(|| anyhow!("Cloudflare R2 secret key is required"))?;

        let credentials = Credentials::new(
            client_id,
            secret_key,
            None,
            None,
            "custom_provider",
        );

        let conf_builder = aws_sdk_s3::config::Builder::new()
            .region(Region::new("us-east-1"))
            .endpoint_url(&url)
            .credentials_provider(credentials)
            .build();

        let client = Client::from_conf(conf_builder);

        Ok(CloudFlareR2 {
            bucket_name,
            client: Arc::new(client),
        })
    }
}

pub struct CloudFlareR2 {
    bucket_name: String,
    client: Arc<Client>,
}

impl CloudFlareR2 {
    pub fn builder() -> R2ManagerBuilder<EmptyUrl, EmptyBucket, EmptyClientId, EmptySecret> {
        R2ManagerBuilder::new()
    }

    pub fn get_bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub async fn create_bucket(&self) -> Result<()> {
        let create_bucket_request = self.client.create_bucket().bucket(&self.bucket_name);

        let result = create_bucket_request.send().await;

        if result.is_ok() {
            info!("Created successfully {}", self.bucket_name);
            Ok(())
        } else {
            error!("Creation of {} failed.", self.bucket_name);
            Err(anyhow!("Failed to create bucket"))
        }
    }

    pub async fn delete_bucket(&self) -> Result<()> {
        let delete_bucket_request = self.client.delete_bucket().bucket(&self.bucket_name);

        let result = delete_bucket_request.send().await;

        if result.is_ok() {
            debug!("{:?}", result.unwrap());
            info!("Deleted successfully {}", self.bucket_name);
            Ok(())
        } else {
            debug!("{:?}", result.unwrap_err());
            error!("Deletion of {} failed.", self.bucket_name);
            Err(anyhow!("Failed to delete bucket"))
        }
    }

    pub async fn put_object(&self, key: &str, body: Vec<u8>) -> Result<String> {
        let content_type = mime_guess::from_path(key).first_or_octet_stream().to_string();
        let put_object_request = self.client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(body))
            .content_type(content_type);

        let _ = put_object_request.send().await?;
        Ok(key.to_string())
    }

    pub async fn delete_object(&self, key: &str) -> Result<bool> {
        let delete_object_request = self.client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(key);

        let _ = delete_object_request.send().await?;
        Ok(true)
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let get_object_request = self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key);
        let response = get_object_request.send().await?;
        let body = response.body.collect().await?.into_bytes().to_vec();
        Ok(body)
    }

    pub async fn download_file(&self, key: &str, dir: &Path) -> Result<String> {
        if !dir.is_dir() {
            bail!("Path {} is not a directory", dir.display());
        }

        let file_path = dir.join(key);
        let parent_dir = file_path
            .parent()
            .ok_or_else(|| anyhow!("Invalid parent dir for {:?}", file_path))?;
        if !parent_dir.exists() {
            create_dir_all(parent_dir)?;
        }
        let get_object_request = self.client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key);

        let result = get_object_request.send().await?;
        let mut data: ByteStream = result.body;
        let file = File::create(&file_path)?;
        let mut buf_writer = BufWriter::new(file);

        while let Some(bytes) = data.try_next().await? {
            buf_writer.write(&bytes)?;
        }
        buf_writer.flush()?;

        Ok(file_path.to_string_lossy().to_string())
    }

    pub async fn list_keys(&self) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let list_objects_request = self.client
                .list_objects_v2()
                .bucket(&self.bucket_name)
                .set_continuation_token(continuation_token.clone());

            let result = list_objects_request.send().await?;
            if let Some(contents) = result.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        keys.push(key);
                    }
                }
            }

            if result.is_truncated.unwrap_or(false) {
                continuation_token = result.next_continuation_token;
            } else {
                break;
            }
        }
        Ok(keys)
    }
}
