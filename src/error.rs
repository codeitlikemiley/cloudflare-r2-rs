//! Error types for the crate.

use std::path::{Path, PathBuf};

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::error::SdkError;

/// Convenient alias for results returned by this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Everything that can go wrong when talking to Cloudflare R2.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A required piece of configuration was absent or blank.
    #[error("missing configuration: {0}")]
    MissingConfig(&'static str),

    /// A configuration value was present but unusable.
    #[error("invalid configuration for `{field}`: {message}")]
    InvalidConfig {
        /// The configuration field at fault.
        field: &'static str,
        /// Why the value was rejected.
        message: String,
    },

    /// An argument handed to an operation was rejected before any request was made.
    #[error("invalid argument `{argument}`: {message}")]
    InvalidArgument {
        /// The argument at fault.
        argument: &'static str,
        /// Why the value was rejected.
        message: String,
    },

    /// The requested object does not exist in the bucket.
    #[error("object `{key}` not found in bucket `{bucket}`")]
    ObjectNotFound {
        /// Bucket that was searched.
        bucket: String,
        /// Key that was not found.
        key: String,
    },

    /// The requested bucket does not exist, or the credentials cannot see it.
    #[error("bucket `{bucket}` not found")]
    BucketNotFound {
        /// Bucket that was not found.
        bucket: String,
    },

    /// The R2 API rejected or failed the request.
    ///
    /// The `Display` message carries the most specific message the service or
    /// transport produced; the full `SdkError` remains reachable as this
    /// error's [`source`](std::error::Error::source), and can be downcast when
    /// you need the typed service error.
    #[error("R2 operation `{operation}` failed: {message}")]
    Api {
        /// The logical operation that failed, e.g. `put_object`.
        operation: &'static str,
        /// The most specific message from the underlying SDK error.
        message: String,
        /// HTTP status code, when the failure reached the service.
        status: Option<u16>,
        /// The underlying SDK error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// A multipart upload could not be completed.
    #[error("multipart upload of `{key}` failed: {message}")]
    Multipart {
        /// Key being uploaded.
        key: String,
        /// What went wrong.
        message: String,
    },

    /// Generating a presigned URL failed.
    #[error("failed to presign `{operation}` for `{key}`: {message}")]
    Presigning {
        /// The operation being presigned.
        operation: &'static str,
        /// Key the URL was for.
        key: String,
        /// What went wrong.
        message: String,
        /// The underlying SDK error.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// A local file involved in a transfer could not be read or written.
    #[error("failed to access local file `{}`: {message}", path.display())]
    File {
        /// The local path at fault.
        path: PathBuf,
        /// What went wrong.
        message: String,
        /// The underlying I/O error, when there was one.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Reading or writing a local file failed.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Reading a response body from the network failed.
    #[error("failed to read response body: {0}")]
    Body(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    /// Returns `true` when the failure means "this thing does not exist".
    ///
    /// Covers both the typed not-found variants and any API error that came
    /// back with a `404` status. Note that a `403` is *not* treated as
    /// not-found: with a scoped R2 API token, "exists but you cannot see it"
    /// and "does not exist" are different answers and this crate does not
    /// conflate them.
    pub fn is_not_found(&self) -> bool {
        match self {
            Error::ObjectNotFound { .. } | Error::BucketNotFound { .. } => true,
            Error::Api { status, .. } => *status == Some(404),
            _ => false,
        }
    }

    /// The HTTP status the service responded with, when there was one.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } => *status,
            Error::ObjectNotFound { .. } | Error::BucketNotFound { .. } => Some(404),
            _ => None,
        }
    }

    pub(crate) fn invalid_argument(argument: &'static str, message: impl Into<String>) -> Self {
        Error::InvalidArgument {
            argument,
            message: message.into(),
        }
    }

    pub(crate) fn multipart(key: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Multipart {
            key: key.into(),
            message: message.into(),
        }
    }

    pub(crate) fn file<E>(path: &Path, message: impl Into<String>, source: Option<E>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Error::File {
            path: path.to_path_buf(),
            message: message.into(),
            source: source.map(|err| Box::new(err) as Box<dyn std::error::Error + Send + Sync>),
        }
    }
}

/// Converts an `SdkError` into our [`Error`], preserving the HTTP status so
/// callers can distinguish "missing" from "broken".
pub(crate) fn from_sdk<E>(operation: &'static str, err: SdkError<E, HttpResponse>) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    let status = match &err {
        SdkError::ServiceError(service) => Some(service.raw().status().as_u16()),
        SdkError::ResponseError(response) => Some(response.raw().status().as_u16()),
        _ => None,
    };

    Error::Api {
        operation,
        message: best_message(&err),
        status,
        source: Box::new(err),
    }
}

/// Picks the most informative message from an error chain.
///
/// `SdkError`'s own `Display` is famously terse — a service failure renders as
/// just "service error" — while the useful text sits further down the chain.
/// The deepest message is therefore the one worth surfacing. It is not
/// concatenated with its ancestors, because the whole chain stays reachable
/// through [`Error::Api`]'s `source` and printing it in both places would
/// duplicate it for anyone walking the chain.
pub(crate) fn best_message(err: &(dyn std::error::Error + 'static)) -> String {
    let mut best = err.to_string();
    let mut source = err.source();
    // Bounded so a pathological (or cyclic) chain cannot spin here.
    for _ in 0..32 {
        let Some(cause) = source else { break };
        let text = cause.to_string();
        if !text.is_empty() {
            best = text;
        }
        source = cause.source();
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Layer {
        message: &'static str,
        source: Option<Box<Layer>>,
    }

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_deref()
                .map(|layer| layer as &(dyn std::error::Error + 'static))
        }
    }

    #[test]
    fn best_message_reaches_the_deepest_cause() {
        let err = Layer {
            message: "service error",
            source: Some(Box::new(Layer {
                message: "unhandled error",
                source: Some(Box::new(Layer {
                    message: "Access Denied",
                    source: None,
                })),
            })),
        };
        assert_eq!(best_message(&err), "Access Denied");
    }

    #[test]
    fn best_message_falls_back_to_the_top_when_alone() {
        let err = Layer {
            message: "dispatch failure",
            source: None,
        };
        assert_eq!(best_message(&err), "dispatch failure");
    }

    #[test]
    fn best_message_skips_empty_causes() {
        let err = Layer {
            message: "outer",
            source: Some(Box::new(Layer {
                message: "",
                source: None,
            })),
        };
        assert_eq!(best_message(&err), "outer");
    }

    #[test]
    fn not_found_covers_typed_variants_and_404() {
        assert!(Error::BucketNotFound { bucket: "b".into() }.is_not_found());
        assert_eq!(
            Error::ObjectNotFound {
                bucket: "b".into(),
                key: "k".into()
            }
            .status(),
            Some(404)
        );
        assert!(!Error::MissingConfig("bucket").is_not_found());
    }

    #[test]
    fn forbidden_is_not_reported_as_not_found() {
        let err = Error::Api {
            operation: "head_bucket",
            message: "Forbidden".into(),
            status: Some(403),
            source: Box::new(Layer {
                message: "Forbidden",
                source: None,
            }),
        };
        assert!(!err.is_not_found());
        assert_eq!(err.status(), Some(403));
    }
}
