//! Error types for the crate.

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

    /// The requested bucket does not exist.
    #[error("bucket `{bucket}` not found")]
    BucketNotFound {
        /// Bucket that was not found.
        bucket: String,
    },

    /// The R2 API rejected or failed the request.
    #[error("R2 operation `{operation}` failed: {message}")]
    Api {
        /// The logical operation that failed, e.g. `put_object`.
        operation: &'static str,
        /// Flattened message from the underlying SDK error.
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
    /// back with a `404` status.
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
        message: flatten_message(&err),
        status,
        source: Box::new(err),
    }
}

/// `SdkError`'s own `Display` is famously terse ("service error"); walk the
/// source chain so the message actually says what happened.
fn flatten_message(err: &(dyn std::error::Error + 'static)) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !text.is_empty() && !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = cause.source();
    }
    message
}
