//! Presigned URLs for handing time-limited access to clients.

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;

use crate::client::R2Client;
use crate::error::{Error, Result};
use crate::object::validate_key;
use crate::types::PresignOptions;

/// The longest expiry R2 (and SigV4) accepts: seven days.
pub const MAX_PRESIGN_EXPIRY: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// A signed request a client can perform without credentials.
///
/// For `GET` and `DELETE` the URL alone is enough. For `PUT`, any headers
/// listed here were part of the signature and the uploading client must send
/// them verbatim or the request is rejected.
#[derive(Debug, Clone)]
pub struct PresignedRequest {
    /// The fully signed URL.
    pub url: String,
    /// HTTP method the client must use.
    pub method: String,
    /// Headers that were signed and must be reproduced by the client.
    pub headers: HashMap<String, String>,
}

impl PresignedRequest {
    /// The signed URL.
    pub fn as_str(&self) -> &str {
        &self.url
    }

    /// Consumes the request, returning just the URL.
    pub fn into_url(self) -> String {
        self.url
    }
}

impl std::fmt::Display for PresignedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.url)
    }
}

impl From<aws_sdk_s3::presigning::PresignedRequest> for PresignedRequest {
    fn from(request: aws_sdk_s3::presigning::PresignedRequest) -> Self {
        let headers = request
            .headers()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();

        PresignedRequest {
            url: request.uri().to_string(),
            method: request.method().to_string(),
            headers,
        }
    }
}

impl R2Client {
    /// Creates a URL that downloads an object, valid for `expires_in`.
    ///
    /// ```no_run
    /// # use std::time::Duration;
    /// # async fn run(client: cloudflare_r2_rs::R2Client) -> cloudflare_r2_rs::Result<()> {
    /// let url = client.presign_get("reports/q3.pdf", Duration::from_secs(300)).await?;
    /// println!("{url}");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn presign_get(&self, key: &str, expires_in: Duration) -> Result<PresignedRequest> {
        self.presign_get_with(key, PresignOptions::new().expires_in(expires_in))
            .await
    }

    /// Creates a download URL, optionally overriding the response headers so
    /// the browser sees a different content type or filename.
    pub async fn presign_get_with(
        &self,
        key: &str,
        options: PresignOptions,
    ) -> Result<PresignedRequest> {
        validate_key(key)?;
        let config = presigning_config("get_object", key, options.expires_in)?;

        self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .set_response_content_type(options.response_content_type)
            .set_response_content_disposition(options.response_content_disposition)
            .presigned(config)
            .await
            .map(Into::into)
            .map_err(|err| presign_error("get_object", key, err))
    }

    /// Creates a URL a client can `PUT` an object to, valid for `expires_in`.
    pub async fn presign_put(&self, key: &str, expires_in: Duration) -> Result<PresignedRequest> {
        self.presign_put_with(key, PresignOptions::new().expires_in(expires_in))
            .await
    }

    /// Creates an upload URL, optionally pinning the `Content-Type` the client
    /// must send.
    pub async fn presign_put_with(
        &self,
        key: &str,
        options: PresignOptions,
    ) -> Result<PresignedRequest> {
        validate_key(key)?;
        let config = presigning_config("put_object", key, options.expires_in)?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .set_content_type(options.content_type)
            .presigned(config)
            .await
            .map(Into::into)
            .map_err(|err| presign_error("put_object", key, err))
    }

    /// Creates a URL that deletes an object, valid for `expires_in`.
    pub async fn presign_delete(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> Result<PresignedRequest> {
        validate_key(key)?;
        let config = presigning_config("delete_object", key, expires_in)?;

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .map(Into::into)
            .map_err(|err| presign_error("delete_object", key, err))
    }

    /// Creates a URL that reads an object's metadata, valid for `expires_in`.
    pub async fn presign_head(&self, key: &str, expires_in: Duration) -> Result<PresignedRequest> {
        validate_key(key)?;
        let config = presigning_config("head_object", key, expires_in)?;

        self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .map(Into::into)
            .map_err(|err| presign_error("head_object", key, err))
    }
}

fn presign_error<E>(operation: &'static str, key: &str, err: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::Presigning {
        operation,
        key: key.to_string(),
        message: crate::error::best_message(&err),
        source: Some(Box::new(err)),
    }
}

fn presigning_config(
    operation: &'static str,
    key: &str,
    expires_in: Duration,
) -> Result<PresigningConfig> {
    if expires_in.is_zero() {
        return Err(Error::Presigning {
            operation,
            key: key.to_string(),
            message: "expiry must be greater than zero".to_string(),
            source: None,
        });
    }

    if expires_in > MAX_PRESIGN_EXPIRY {
        return Err(Error::Presigning {
            operation,
            key: key.to_string(),
            message: format!(
                "expiry of {}s exceeds the maximum of {}s (7 days)",
                expires_in.as_secs(),
                MAX_PRESIGN_EXPIRY.as_secs()
            ),
            source: None,
        });
    }

    PresigningConfig::expires_in(expires_in).map_err(|err| Error::Presigning {
        operation,
        key: key.to_string(),
        message: err.to_string(),
        source: Some(Box::new(err)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::R2Client;

    fn client() -> R2Client {
        R2Client::builder()
            .account_id("acct")
            .bucket("bucket")
            .access_key_id("access-key")
            .secret_access_key("secret-key")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn presigns_a_get_url() {
        let request = client()
            .presign_get("photos/cat.png", Duration::from_secs(600))
            .await
            .unwrap();
        assert_eq!(request.method, "GET");
        assert!(request
            .url
            .starts_with("https://acct.r2.cloudflarestorage.com/bucket/photos/cat.png?"));
        assert!(request.url.contains("X-Amz-Signature="));
        assert!(request.url.contains("X-Amz-Expires=600"));
    }

    #[tokio::test]
    async fn presigns_a_put_url() {
        let request = client()
            .presign_put("uploads/new.bin", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(request.method, "PUT");
        assert!(request.url.contains("X-Amz-Signature="));
    }

    #[tokio::test]
    async fn presigns_a_delete_url() {
        let request = client()
            .presign_delete("uploads/new.bin", Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(request.method, "DELETE");
    }

    #[tokio::test]
    async fn response_overrides_reach_the_query_string() {
        let request = client()
            .presign_get_with(
                "reports/q3.pdf",
                PresignOptions::new()
                    .expires_in(Duration::from_secs(60))
                    .response_content_disposition("attachment; filename=\"q3.pdf\""),
            )
            .await
            .unwrap();
        assert!(request.url.contains("response-content-disposition="));
    }

    #[tokio::test]
    async fn rejects_expiry_beyond_seven_days() {
        let err = client()
            .presign_get("a.txt", MAX_PRESIGN_EXPIRY + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Presigning { .. }));
    }

    #[tokio::test]
    async fn rejects_zero_expiry() {
        assert!(client().presign_get("a.txt", Duration::ZERO).await.is_err());
    }

    #[tokio::test]
    async fn rejects_empty_key() {
        assert!(client()
            .presign_get("", Duration::from_secs(60))
            .await
            .is_err());
    }
}
