//! The client, its typestate builder, and bucket-level operations.

use std::marker::PhantomData;
use std::sync::Arc;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{BucketLifecycleConfiguration, CorsConfiguration, CorsRule, LifecycleRule};
use aws_sdk_s3::Client;

use crate::config::{endpoint_for_account, Jurisdiction, R2Config, DEFAULT_REGION};
use crate::error::{from_sdk, Error, Result};
use crate::types::BucketSummary;

/// Builder state: the endpoint has not been supplied yet.
#[derive(Debug, Default)]
pub struct NoEndpoint;
/// Builder state: the endpoint has been supplied.
#[derive(Debug, Default)]
pub struct HasEndpoint;
/// Builder state: the bucket has not been supplied yet.
#[derive(Debug, Default)]
pub struct NoBucket;
/// Builder state: the bucket has been supplied.
#[derive(Debug, Default)]
pub struct HasBucket;
/// Builder state: the access key ID has not been supplied yet.
#[derive(Debug, Default)]
pub struct NoAccessKey;
/// Builder state: the access key ID has been supplied.
#[derive(Debug, Default)]
pub struct HasAccessKey;
/// Builder state: the secret access key has not been supplied yet.
#[derive(Debug, Default)]
pub struct NoSecretKey;
/// Builder state: the secret access key has been supplied.
#[derive(Debug, Default)]
pub struct HasSecretKey;

/// Typestate builder for [`R2Client`].
///
/// [`build`](R2ClientBuilder::build) only exists once the endpoint, bucket,
/// access key and secret key have all been set, so a half-configured client is
/// a compile error rather than a runtime one.
///
/// ```
/// use cloudflare_r2_rs::R2Client;
///
/// let client = R2Client::builder()
///     .account_id("0123456789abcdef")
///     .bucket("media")
///     .access_key_id("access-key")
///     .secret_access_key("secret-key")
///     .build()
///     .unwrap();
/// assert_eq!(client.bucket(), "media");
/// ```
#[derive(Debug, Default)]
pub struct R2ClientBuilder<EndpointState, BucketState, AccessKeyState, SecretKeyState> {
    endpoint: Option<String>,
    bucket: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    jurisdiction: Jurisdiction,
    _endpoint: PhantomData<EndpointState>,
    _bucket: PhantomData<BucketState>,
    _access_key: PhantomData<AccessKeyState>,
    _secret_key: PhantomData<SecretKeyState>,
}

impl R2ClientBuilder<NoEndpoint, NoBucket, NoAccessKey, NoSecretKey> {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Rebuilds the builder with new type-state parameters, moving every field.
///
/// Rust cannot change just the `PhantomData` parameters in place, so each
/// setter reconstructs the struct; this macro keeps that from being four
/// near-identical copies of the same eleven-field literal.
macro_rules! transition {
    ($self:ident) => {
        R2ClientBuilder {
            endpoint: $self.endpoint,
            bucket: $self.bucket,
            access_key_id: $self.access_key_id,
            secret_access_key: $self.secret_access_key,
            region: $self.region,
            jurisdiction: $self.jurisdiction,
            _endpoint: PhantomData,
            _bucket: PhantomData,
            _access_key: PhantomData,
            _secret_key: PhantomData,
        }
    };
}

impl<EndpointState, BucketState, AccessKeyState, SecretKeyState>
    R2ClientBuilder<EndpointState, BucketState, AccessKeyState, SecretKeyState>
{
    /// Sets the S3-compatible endpoint URL directly.
    pub fn endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> R2ClientBuilder<HasEndpoint, BucketState, AccessKeyState, SecretKeyState> {
        self.endpoint = Some(endpoint.into());
        transition!(self)
    }

    /// Derives the endpoint from a Cloudflare account ID.
    ///
    /// Honours any [`jurisdiction`](R2ClientBuilder::jurisdiction) set before
    /// or after this call.
    pub fn account_id(
        mut self,
        account_id: impl AsRef<str>,
    ) -> R2ClientBuilder<HasEndpoint, BucketState, AccessKeyState, SecretKeyState> {
        self.endpoint = Some(endpoint_for_account(account_id.as_ref(), self.jurisdiction));
        transition!(self)
    }

    /// Binds the client to a bucket.
    pub fn bucket(
        mut self,
        bucket: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, HasBucket, AccessKeyState, SecretKeyState> {
        self.bucket = Some(bucket.into());
        transition!(self)
    }

    /// Sets the R2 access key ID.
    pub fn access_key_id(
        mut self,
        access_key_id: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, BucketState, HasAccessKey, SecretKeyState> {
        self.access_key_id = Some(access_key_id.into());
        transition!(self)
    }

    /// Sets the R2 secret access key.
    pub fn secret_access_key(
        mut self,
        secret_access_key: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, BucketState, AccessKeyState, HasSecretKey> {
        self.secret_access_key = Some(secret_access_key.into());
        transition!(self)
    }

    /// Selects a data-residency jurisdiction.
    ///
    /// Only affects endpoints derived via [`account_id`](R2ClientBuilder::account_id);
    /// an explicit [`endpoint`](R2ClientBuilder::endpoint) is used verbatim. If
    /// `account_id` was already called, the endpoint is recomputed.
    pub fn jurisdiction(mut self, jurisdiction: Jurisdiction) -> Self {
        self.jurisdiction = jurisdiction;
        self
    }

    /// Overrides the region used for request signing. Defaults to `auto`.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
}

impl R2ClientBuilder<HasEndpoint, HasBucket, HasAccessKey, HasSecretKey> {
    /// Builds the client.
    ///
    /// Returns [`Error::MissingConfig`] if any value was set to an empty
    /// string — the typestate guarantees the setters were *called*, not that
    /// what they were given was usable.
    pub fn build(self) -> Result<R2Client> {
        let endpoint = require(self.endpoint, "endpoint")?;
        let bucket = require(self.bucket, "bucket")?;
        let access_key_id = require(self.access_key_id, "access_key_id")?;
        let secret_access_key = require(self.secret_access_key, "secret_access_key")?;

        R2Client::from_config(R2Config {
            endpoint,
            access_key_id,
            secret_access_key,
            bucket,
            region: self.region.unwrap_or_else(|| DEFAULT_REGION.to_string()),
        })
    }
}

fn require(value: Option<String>, field: &'static str) -> Result<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or(Error::MissingConfig(field))
}

/// A client bound to one R2 bucket.
///
/// Cloning is cheap — clones share the underlying connection pool — so pass
/// clones around rather than wrapping this in another [`Arc`].
#[derive(Debug, Clone)]
pub struct R2Client {
    pub(crate) client: Arc<Client>,
    pub(crate) bucket: String,
    endpoint: String,
}

impl R2Client {
    /// Starts building a client.
    pub fn builder() -> R2ClientBuilder<NoEndpoint, NoBucket, NoAccessKey, NoSecretKey> {
        R2ClientBuilder::new()
    }

    /// Builds a client from fully resolved configuration.
    pub fn from_config(config: R2Config) -> Result<Self> {
        if config.endpoint.trim().is_empty() {
            return Err(Error::MissingConfig("endpoint"));
        }
        if config.bucket.trim().is_empty() {
            return Err(Error::MissingConfig("bucket"));
        }

        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key,
            None,
            None,
            "cloudflare-r2-rs",
        );

        let s3_config = aws_sdk_s3::config::Builder::new()
            .region(Region::new(config.region))
            .endpoint_url(config.endpoint.trim_end_matches('/').to_string())
            .credentials_provider(credentials)
            // R2 does not support virtual-hosted-style addressing on the
            // S3-compatible endpoint; requests must be path-style.
            .force_path_style(true)
            .build();

        Ok(R2Client {
            client: Arc::new(Client::from_conf(s3_config)),
            bucket: config.bucket,
            endpoint: config.endpoint,
        })
    }

    /// Builds a client from environment variables. See [`R2Config::from_env`].
    pub fn from_env() -> Result<Self> {
        Self::from_config(R2Config::from_env()?)
    }

    /// The bucket this client is bound to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The endpoint this client talks to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns a client for a different bucket on the same account, reusing
    /// this client's connection pool and credentials.
    pub fn with_bucket(&self, bucket: impl Into<String>) -> Self {
        R2Client {
            client: Arc::clone(&self.client),
            bucket: bucket.into(),
            endpoint: self.endpoint.clone(),
        }
    }

    /// The underlying `aws-sdk-s3` client, for operations this crate does not
    /// wrap.
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Creates this client's bucket.
    pub async fn create_bucket(&self) -> Result<()> {
        self.client
            .create_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| from_sdk("create_bucket", err))?;
        Ok(())
    }

    /// Deletes this client's bucket. The bucket must already be empty.
    pub async fn delete_bucket(&self) -> Result<()> {
        self.client
            .delete_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| from_sdk("delete_bucket", err))?;
        Ok(())
    }

    /// Reports whether this client's bucket exists and is reachable with the
    /// configured credentials.
    pub async fn bucket_exists(&self) -> Result<bool> {
        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                let err = from_sdk("head_bucket", err);
                if err.is_not_found() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Lists every bucket on the account.
    pub async fn list_buckets(&self) -> Result<Vec<BucketSummary>> {
        let response = self
            .client
            .list_buckets()
            .send()
            .await
            .map_err(|err| from_sdk("list_buckets", err))?;

        Ok(response
            .buckets
            .unwrap_or_default()
            .into_iter()
            .filter_map(|bucket| {
                bucket.name.map(|name| BucketSummary {
                    name,
                    creation_date: bucket.creation_date,
                })
            })
            .collect())
    }

    /// Reads the bucket's CORS rules.
    ///
    /// Returns an empty vector when no CORS configuration is set, rather than
    /// the `NoSuchCORSConfiguration` error the API reports.
    pub async fn get_cors(&self) -> Result<Vec<CorsRule>> {
        match self
            .client
            .get_bucket_cors()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(response) => Ok(response.cors_rules.unwrap_or_default()),
            Err(err) => {
                let err = from_sdk("get_bucket_cors", err);
                if err.is_not_found() {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Replaces the bucket's CORS rules.
    pub async fn put_cors(&self, rules: Vec<CorsRule>) -> Result<()> {
        if rules.is_empty() {
            return Err(Error::invalid_argument(
                "rules",
                "at least one CORS rule is required; use delete_cors() to clear them",
            ));
        }

        let configuration = CorsConfiguration::builder()
            .set_cors_rules(Some(rules))
            .build()
            .map_err(|err| Error::invalid_argument("rules", err.to_string()))?;

        self.client
            .put_bucket_cors()
            .bucket(&self.bucket)
            .cors_configuration(configuration)
            .send()
            .await
            .map_err(|err| from_sdk("put_bucket_cors", err))?;
        Ok(())
    }

    /// Removes the bucket's CORS configuration entirely.
    pub async fn delete_cors(&self) -> Result<()> {
        self.client
            .delete_bucket_cors()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| from_sdk("delete_bucket_cors", err))?;
        Ok(())
    }

    /// Reads the bucket's lifecycle rules.
    ///
    /// Returns an empty vector when no lifecycle configuration is set.
    pub async fn get_lifecycle(&self) -> Result<Vec<LifecycleRule>> {
        match self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(response) => Ok(response.rules.unwrap_or_default()),
            Err(err) => {
                let err = from_sdk("get_bucket_lifecycle_configuration", err);
                if err.is_not_found() {
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    /// Replaces the bucket's lifecycle rules.
    pub async fn put_lifecycle(&self, rules: Vec<LifecycleRule>) -> Result<()> {
        if rules.is_empty() {
            return Err(Error::invalid_argument(
                "rules",
                "at least one lifecycle rule is required; use delete_lifecycle() to clear them",
            ));
        }

        let configuration = BucketLifecycleConfiguration::builder()
            .set_rules(Some(rules))
            .build()
            .map_err(|err| Error::invalid_argument("rules", err.to_string()))?;

        self.client
            .put_bucket_lifecycle_configuration()
            .bucket(&self.bucket)
            .lifecycle_configuration(configuration)
            .send()
            .await
            .map_err(|err| from_sdk("put_bucket_lifecycle_configuration", err))?;
        Ok(())
    }

    /// Removes the bucket's lifecycle configuration entirely.
    pub async fn delete_lifecycle(&self) -> Result<()> {
        self.client
            .delete_bucket_lifecycle()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| from_sdk("delete_bucket_lifecycle", err))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> R2Client {
        R2Client::builder()
            .account_id("acct")
            .bucket("bucket")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap()
    }

    #[test]
    fn builds_from_account_id() {
        let client = client();
        assert_eq!(client.bucket(), "bucket");
        assert_eq!(client.endpoint(), "https://acct.r2.cloudflarestorage.com");
    }

    #[test]
    fn jurisdiction_applies_to_derived_endpoint() {
        let client = R2Client::builder()
            .jurisdiction(Jurisdiction::Eu)
            .account_id("acct")
            .bucket("bucket")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap();
        assert_eq!(
            client.endpoint(),
            "https://acct.eu.r2.cloudflarestorage.com"
        );
    }

    #[test]
    fn explicit_endpoint_is_used_verbatim() {
        let client = R2Client::builder()
            .endpoint("http://localhost:9000")
            .bucket("bucket")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap();
        assert_eq!(client.endpoint(), "http://localhost:9000");
    }

    #[test]
    fn blank_values_are_rejected_at_build_time() {
        let err = R2Client::builder()
            .endpoint("https://acct.r2.cloudflarestorage.com")
            .bucket("   ")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::MissingConfig("bucket")));
    }

    #[test]
    fn with_bucket_keeps_endpoint() {
        let other = client().with_bucket("other");
        assert_eq!(other.bucket(), "other");
        assert_eq!(other.endpoint(), "https://acct.r2.cloudflarestorage.com");
    }

    #[tokio::test]
    async fn empty_cors_rules_are_rejected_before_any_request() {
        let err = client().put_cors(Vec::new()).await.unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }

    #[tokio::test]
    async fn empty_lifecycle_rules_are_rejected_before_any_request() {
        let err = client().put_lifecycle(Vec::new()).await.unwrap_err();
        assert!(matches!(err, Error::InvalidArgument { .. }));
    }
}
