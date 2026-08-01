//! The client, its typestate builder, and bucket-level operations.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{Credentials, Region, RequestChecksumCalculation};
use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_s3::types::{BucketLifecycleConfiguration, CorsConfiguration, CorsRule, LifecycleRule};
use aws_sdk_s3::Client;

use crate::config::{endpoint_for_account, Jurisdiction, R2Config, DEFAULT_REGION};
use crate::error::{from_sdk, Error, Result};
use crate::types::BucketSummary;

/// Connect timeout applied unless the caller overrides it.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
/// Setter order does not matter. In particular the endpoint is resolved when
/// `build()` is called, so [`jurisdiction`](R2ClientBuilder::jurisdiction) has
/// the same effect before or after [`account_id`](R2ClientBuilder::account_id).
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
    account_id: Option<String>,
    bucket: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    jurisdiction: Jurisdiction,
    retry_config: Option<RetryConfig>,
    timeout_config: Option<TimeoutConfig>,
    _endpoint: PhantomData<EndpointState>,
    _bucket: PhantomData<BucketState>,
    _access_key: PhantomData<AccessKeyState>,
    _secret_key: PhantomData<SecretKeyState>,
}

impl R2ClientBuilder<NoEndpoint, NoBucket, NoAccessKey, NoSecretKey> {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Rebuilds the builder with new type-state parameters, moving every field.
///
/// Rust cannot change just the `PhantomData` parameters in place, so each
/// setter reconstructs the struct; this macro keeps that from being four
/// near-identical copies of the same field list.
macro_rules! transition {
    ($self:ident) => {
        R2ClientBuilder {
            endpoint: $self.endpoint,
            account_id: $self.account_id,
            bucket: $self.bucket,
            access_key_id: $self.access_key_id,
            secret_access_key: $self.secret_access_key,
            region: $self.region,
            jurisdiction: $self.jurisdiction,
            retry_config: $self.retry_config,
            timeout_config: $self.timeout_config,
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
    ///
    /// An explicit endpoint wins over one derived from
    /// [`account_id`](R2ClientBuilder::account_id), whichever was set first.
    #[must_use]
    pub fn endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> R2ClientBuilder<HasEndpoint, BucketState, AccessKeyState, SecretKeyState> {
        self.endpoint = Some(endpoint.into());
        transition!(self)
    }

    /// Derives the endpoint from a Cloudflare account ID.
    ///
    /// The endpoint is computed during [`build`](R2ClientBuilder::build), so a
    /// [`jurisdiction`](R2ClientBuilder::jurisdiction) set after this call is
    /// still honoured.
    #[must_use]
    pub fn account_id(
        mut self,
        account_id: impl Into<String>,
    ) -> R2ClientBuilder<HasEndpoint, BucketState, AccessKeyState, SecretKeyState> {
        self.account_id = Some(account_id.into());
        transition!(self)
    }

    /// Binds the client to a bucket.
    #[must_use]
    pub fn bucket(
        mut self,
        bucket: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, HasBucket, AccessKeyState, SecretKeyState> {
        self.bucket = Some(bucket.into());
        transition!(self)
    }

    /// Sets the R2 access key ID.
    #[must_use]
    pub fn access_key_id(
        mut self,
        access_key_id: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, BucketState, HasAccessKey, SecretKeyState> {
        self.access_key_id = Some(access_key_id.into());
        transition!(self)
    }

    /// Sets the R2 secret access key.
    #[must_use]
    pub fn secret_access_key(
        mut self,
        secret_access_key: impl Into<String>,
    ) -> R2ClientBuilder<EndpointState, BucketState, AccessKeyState, HasSecretKey> {
        self.secret_access_key = Some(secret_access_key.into());
        transition!(self)
    }

    /// Selects a data-residency jurisdiction.
    ///
    /// Only affects endpoints derived from an
    /// [`account_id`](R2ClientBuilder::account_id); an explicit
    /// [`endpoint`](R2ClientBuilder::endpoint) is used verbatim.
    #[must_use]
    pub fn jurisdiction(mut self, jurisdiction: Jurisdiction) -> Self {
        self.jurisdiction = jurisdiction;
        self
    }

    /// Overrides the region used for request signing. Defaults to `auto`.
    #[must_use]
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Overrides the retry policy.
    ///
    /// Defaults to the SDK's standard policy. Pass `RetryConfig::disabled()`
    /// when the caller does its own retrying.
    #[must_use]
    pub fn retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = Some(retry_config);
        self
    }

    /// Overrides the timeout policy.
    ///
    /// Defaults to a 5 second connect timeout and no operation timeout, since
    /// a single upload or download can legitimately run for a long time.
    #[must_use]
    pub fn timeout_config(mut self, timeout_config: TimeoutConfig) -> Self {
        self.timeout_config = Some(timeout_config);
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
        let endpoint = match require_opt(self.endpoint) {
            Some(endpoint) => endpoint,
            None => {
                let account_id = require(self.account_id, "account_id")?;
                endpoint_for_account(&account_id, self.jurisdiction)
            }
        };

        let bucket = require(self.bucket, "bucket")?;
        let access_key_id = require(self.access_key_id, "access_key_id")?;
        let secret_access_key = require(self.secret_access_key, "secret_access_key")?;

        R2Client::build(
            R2Config {
                endpoint,
                access_key_id,
                secret_access_key,
                bucket,
                region: self.region.unwrap_or_else(|| DEFAULT_REGION.to_string()),
            },
            self.retry_config,
            self.timeout_config,
        )
    }
}

fn require_opt(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn require(value: Option<String>, field: &'static str) -> Result<String> {
    require_opt(value).ok_or(Error::MissingConfig(field))
}

/// Extracts the service-side error code, e.g. `NoSuchCORSConfiguration`.
fn service_code<E>(err: &SdkError<E, HttpResponse>) -> Option<&str>
where
    E: ProvideErrorMetadata,
{
    match err {
        SdkError::ServiceError(service) => service.err().code(),
        _ => None,
    }
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
    #[must_use]
    pub fn builder() -> R2ClientBuilder<NoEndpoint, NoBucket, NoAccessKey, NoSecretKey> {
        R2ClientBuilder::new()
    }

    /// Builds a client from fully resolved configuration, with default retry
    /// and timeout policies.
    pub fn from_config(config: R2Config) -> Result<Self> {
        Self::build(config, None, None)
    }

    fn build(
        config: R2Config,
        retry_config: Option<RetryConfig>,
        timeout_config: Option<TimeoutConfig>,
    ) -> Result<Self> {
        let endpoint = config.endpoint.trim().trim_end_matches('/').to_string();
        if endpoint.is_empty() {
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
            .endpoint_url(endpoint.clone())
            .credentials_provider(credentials)
            // R2's S3-compatible endpoint addresses buckets by path, not by
            // virtual host.
            .force_path_style(true)
            // Only send checksums where the operation requires them. Left on
            // "when supported", the SDK streams uploads as `aws-chunked` with a
            // trailing CRC32; R2 does not strip that framing header, so the
            // stored object ends up with `Content-Encoding: aws-chunked` and
            // becomes undecodable to browsers. Required-checksum operations
            // such as DeleteObjects are unaffected.
            .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
            .retry_config(retry_config.unwrap_or_else(RetryConfig::standard))
            .timeout_config(timeout_config.unwrap_or_else(|| {
                // No operation timeout: a large upload or download is allowed
                // to take as long as it takes.
                TimeoutConfig::builder()
                    .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
                    .build()
            }))
            .build();

        Ok(R2Client {
            client: Arc::new(Client::from_conf(s3_config)),
            bucket: config.bucket,
            endpoint,
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

    /// The endpoint this client talks to, normalized without a trailing slash.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Returns a client for a different bucket on the same account, reusing
    /// this client's connection pool and credentials.
    #[must_use]
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

    /// Turns a 404 on a bucket operation into [`Error::BucketNotFound`].
    fn map_bucket_error<E>(&self, operation: &'static str, err: SdkError<E, HttpResponse>) -> Error
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let err = from_sdk(operation, err);
        if err.is_not_found() {
            Error::BucketNotFound {
                bucket: self.bucket.clone(),
            }
        } else {
            err
        }
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
            .map_err(|err| self.map_bucket_error("delete_bucket", err))?;
        Ok(())
    }

    /// Reports whether this client's bucket exists.
    ///
    /// A `403` is an error, not `false`: with a bucket-scoped R2 API token,
    /// "exists but this token cannot see it" is a different answer from "does
    /// not exist", and reporting the former as `false` would be a lie.
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
    ///
    /// Requires an account-scoped API token; a bucket-scoped token gets a
    /// permission error.
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
    /// Returns an empty vector when the bucket exists but has no CORS
    /// configuration, which the API reports as the `NoSuchCORSConfiguration`
    /// error. Any other failure — including a missing bucket — is returned as
    /// an error rather than as "no rules".
    pub async fn get_cors(&self) -> Result<Vec<CorsRule>> {
        match self
            .client
            .get_bucket_cors()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(response) => Ok(response.cors_rules.unwrap_or_default()),
            Err(err) if service_code(&err) == Some("NoSuchCORSConfiguration") => Ok(Vec::new()),
            Err(err) => Err(self.map_bucket_error("get_bucket_cors", err)),
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
            .map_err(|err| self.map_bucket_error("put_bucket_cors", err))?;
        Ok(())
    }

    /// Removes the bucket's CORS configuration entirely.
    pub async fn delete_cors(&self) -> Result<()> {
        self.client
            .delete_bucket_cors()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| self.map_bucket_error("delete_bucket_cors", err))?;
        Ok(())
    }

    /// Reads the bucket's lifecycle rules.
    ///
    /// Returns an empty vector when the bucket exists but has no lifecycle
    /// configuration, which the API reports as the
    /// `NoSuchLifecycleConfiguration` error. Any other failure — including a
    /// missing bucket — is returned as an error rather than as "no rules".
    pub async fn get_lifecycle(&self) -> Result<Vec<LifecycleRule>> {
        match self
            .client
            .get_bucket_lifecycle_configuration()
            .bucket(&self.bucket)
            .send()
            .await
        {
            Ok(response) => Ok(response.rules.unwrap_or_default()),
            Err(err) if service_code(&err) == Some("NoSuchLifecycleConfiguration") => {
                Ok(Vec::new())
            }
            Err(err) => Err(self.map_bucket_error("get_bucket_lifecycle_configuration", err)),
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
            .map_err(|err| self.map_bucket_error("put_bucket_lifecycle_configuration", err))?;
        Ok(())
    }

    /// Removes the bucket's lifecycle configuration entirely.
    pub async fn delete_lifecycle(&self) -> Result<()> {
        self.client
            .delete_bucket_lifecycle()
            .bucket(&self.bucket)
            .send()
            .await
            .map_err(|err| self.map_bucket_error("delete_bucket_lifecycle", err))?;
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
    fn jurisdiction_applies_before_account_id() {
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
    fn jurisdiction_applies_after_account_id() {
        // Order must not matter: the endpoint is resolved at build() time.
        let client = R2Client::builder()
            .account_id("acct")
            .jurisdiction(Jurisdiction::Eu)
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
    fn explicit_endpoint_beats_account_id_in_either_order() {
        for client in [
            R2Client::builder()
                .account_id("acct")
                .endpoint("http://localhost:9000")
                .bucket("b")
                .access_key_id("k")
                .secret_access_key("s")
                .build()
                .unwrap(),
            R2Client::builder()
                .endpoint("http://localhost:9000")
                .account_id("acct")
                .bucket("b")
                .access_key_id("k")
                .secret_access_key("s")
                .build()
                .unwrap(),
        ] {
            assert_eq!(client.endpoint(), "http://localhost:9000");
        }
    }

    #[test]
    fn endpoint_is_normalized_without_a_trailing_slash() {
        let client = R2Client::builder()
            .endpoint("https://acct.r2.cloudflarestorage.com/")
            .bucket("bucket")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap();
        assert_eq!(client.endpoint(), "https://acct.r2.cloudflarestorage.com");
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
    fn blank_account_id_without_endpoint_is_rejected() {
        let err = R2Client::builder()
            .account_id("  ")
            .bucket("bucket")
            .access_key_id("key")
            .secret_access_key("secret")
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::MissingConfig("account_id")));
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
