//! Endpoint and credential configuration.

use crate::error::{Error, Result};

/// The default region string R2 expects. R2 is not region-partitioned the way
/// S3 is, but the S3 protocol requires *some* region for request signing.
pub const DEFAULT_REGION: &str = "auto";

/// Data-residency jurisdiction for an R2 bucket.
///
/// Buckets created within a jurisdiction are only reachable through that
/// jurisdiction's endpoint, so this must match how the bucket was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Jurisdiction {
    /// The default, unrestricted endpoint.
    #[default]
    Default,
    /// Data stored and processed in the European Union.
    Eu,
    /// The FedRAMP-authorized environment.
    FedRamp,
}

impl Jurisdiction {
    /// The hostname infix R2 uses for this jurisdiction, if any.
    fn host_segment(self) -> Option<&'static str> {
        match self {
            Jurisdiction::Default => None,
            Jurisdiction::Eu => Some("eu"),
            Jurisdiction::FedRamp => Some("fedramp"),
        }
    }

    /// Parses a jurisdiction from its Cloudflare name (`eu`, `fedramp`, `default`).
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "default" => Ok(Jurisdiction::Default),
            "eu" => Ok(Jurisdiction::Eu),
            "fedramp" => Ok(Jurisdiction::FedRamp),
            other => Err(Error::InvalidConfig {
                field: "jurisdiction",
                message: format!("unknown jurisdiction `{other}`, expected default, eu or fedramp"),
            }),
        }
    }
}

/// Builds the S3-compatible endpoint URL for a Cloudflare account.
///
/// ```
/// use cloudflare_r2_rs::{endpoint_for_account, Jurisdiction};
///
/// assert_eq!(
///     endpoint_for_account("abc123", Jurisdiction::Default),
///     "https://abc123.r2.cloudflarestorage.com"
/// );
/// assert_eq!(
///     endpoint_for_account("abc123", Jurisdiction::Eu),
///     "https://abc123.eu.r2.cloudflarestorage.com"
/// );
/// ```
pub fn endpoint_for_account(account_id: &str, jurisdiction: Jurisdiction) -> String {
    match jurisdiction.host_segment() {
        Some(segment) => format!("https://{account_id}.{segment}.r2.cloudflarestorage.com"),
        None => format!("https://{account_id}.r2.cloudflarestorage.com"),
    }
}

/// Fully resolved configuration for an [`R2Client`](crate::R2Client).
///
/// `Debug` deliberately redacts [`secret_access_key`](R2Config::secret_access_key):
/// configuration structs end up in tracing spans and panic messages, and a
/// live credential must not ride along. Read the field directly if you
/// genuinely need its value.
#[derive(Clone)]
pub struct R2Config {
    /// S3-compatible endpoint URL.
    pub endpoint: String,
    /// R2 access key ID.
    pub access_key_id: String,
    /// R2 secret access key.
    pub secret_access_key: String,
    /// Bucket this client is bound to.
    pub bucket: String,
    /// Region used for request signing.
    pub region: String,
}

impl std::fmt::Debug for R2Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("R2Config")
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .finish()
    }
}

impl R2Config {
    /// Reads configuration from the process environment.
    ///
    /// Required variables:
    ///
    /// - `R2_ACCESS_KEY_ID` (or `CLOUDFLARE_CLIENT_ID`)
    /// - `R2_SECRET_ACCESS_KEY` (or `CLOUDFLARE_SECRET_KEY`)
    /// - `R2_BUCKET` (or `CLOUDFLARE_BUCKET_NAME`)
    /// - `R2_ENDPOINT` (or `CLOUDFLARE_URL`), otherwise `R2_ACCOUNT_ID` (or
    ///   `CLOUDFLARE_ACCOUNT_ID`), from which the endpoint is derived
    ///
    /// An explicit endpoint takes precedence: if both are set, `R2_ACCOUNT_ID`
    /// and `R2_JURISDICTION` are ignored.
    ///
    /// Optional variables: `R2_JURISDICTION` (`default`, `eu`, `fedramp`) and
    /// `R2_REGION`.
    pub fn from_env() -> Result<Self> {
        let access_key_id = env_any(&["R2_ACCESS_KEY_ID", "CLOUDFLARE_CLIENT_ID"])
            .ok_or(Error::MissingConfig("R2_ACCESS_KEY_ID"))?;
        let secret_access_key = env_any(&["R2_SECRET_ACCESS_KEY", "CLOUDFLARE_SECRET_KEY"])
            .ok_or(Error::MissingConfig("R2_SECRET_ACCESS_KEY"))?;
        let bucket = env_any(&["R2_BUCKET", "CLOUDFLARE_BUCKET_NAME"])
            .ok_or(Error::MissingConfig("R2_BUCKET"))?;

        let jurisdiction = match env_any(&["R2_JURISDICTION"]) {
            Some(value) => Jurisdiction::parse(&value)?,
            None => Jurisdiction::Default,
        };

        let endpoint = match env_any(&["R2_ENDPOINT", "CLOUDFLARE_URL"]) {
            Some(endpoint) => endpoint,
            None => {
                let account_id = env_any(&["R2_ACCOUNT_ID", "CLOUDFLARE_ACCOUNT_ID"])
                    .ok_or(Error::MissingConfig("R2_ACCOUNT_ID or R2_ENDPOINT"))?;
                endpoint_for_account(&account_id, jurisdiction)
            }
        };

        Ok(R2Config {
            endpoint,
            access_key_id,
            secret_access_key,
            bucket,
            region: env_any(&["R2_REGION"]).unwrap_or_else(|| DEFAULT_REGION.to_string()),
        })
    }
}

/// Returns the first of `names` that is set to a non-blank value.
fn env_any(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_the_secret_key() {
        let config = R2Config {
            endpoint: "https://acct.r2.cloudflarestorage.com".into(),
            access_key_id: "public-id".into(),
            secret_access_key: "super-secret-value".into(),
            bucket: "bucket".into(),
            region: "auto".into(),
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-value"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        // The non-sensitive fields are still useful for debugging.
        assert!(rendered.contains("public-id"));
        assert!(rendered.contains("bucket"));
    }

    #[test]
    fn builds_default_endpoint() {
        assert_eq!(
            endpoint_for_account("acct", Jurisdiction::Default),
            "https://acct.r2.cloudflarestorage.com"
        );
    }

    #[test]
    fn builds_jurisdiction_endpoints() {
        assert_eq!(
            endpoint_for_account("acct", Jurisdiction::Eu),
            "https://acct.eu.r2.cloudflarestorage.com"
        );
        assert_eq!(
            endpoint_for_account("acct", Jurisdiction::FedRamp),
            "https://acct.fedramp.r2.cloudflarestorage.com"
        );
    }

    #[test]
    fn parses_jurisdictions_case_insensitively() {
        assert_eq!(Jurisdiction::parse("EU").unwrap(), Jurisdiction::Eu);
        assert_eq!(Jurisdiction::parse("  ").unwrap(), Jurisdiction::Default);
        assert_eq!(
            Jurisdiction::parse("FedRAMP").unwrap(),
            Jurisdiction::FedRamp
        );
    }

    #[test]
    fn rejects_unknown_jurisdiction() {
        let err = Jurisdiction::parse("mars").unwrap_err();
        assert!(err.to_string().contains("mars"));
    }
}
