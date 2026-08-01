//! Request options and response types shared across operations.

use std::collections::HashMap;
use std::time::Duration;

pub use aws_sdk_s3::primitives::DateTime;

/// Default part size for multipart uploads (8 MiB).
pub const DEFAULT_PART_SIZE: u64 = 8 * 1024 * 1024;

/// Smallest part size S3-compatible APIs accept for a non-final part (5 MiB).
pub const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Largest number of parts a single multipart upload may have.
pub const MAX_PARTS: u64 = 10_000;

/// Files at or above this size are uploaded with multipart by default (16 MiB).
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// Largest number of keys a single `DeleteObjects` request accepts.
pub const MAX_DELETE_BATCH: usize = 1000;

/// Optional headers and user metadata to store alongside an object.
///
/// ```
/// use cloudflare_r2_rs::PutOptions;
///
/// let options = PutOptions::new()
///     .content_type("application/json")
///     .cache_control("public, max-age=3600")
///     .metadata("uploaded-by", "worker-7");
/// assert_eq!(options.content_type.as_deref(), Some("application/json"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// MIME type. When unset it is guessed from the key's extension.
    pub content_type: Option<String>,
    /// `Cache-Control` header served with the object.
    pub cache_control: Option<String>,
    /// `Content-Disposition` header served with the object.
    pub content_disposition: Option<String>,
    /// `Content-Encoding` header served with the object.
    pub content_encoding: Option<String>,
    /// `Content-Language` header served with the object.
    pub content_language: Option<String>,
    /// Arbitrary user metadata, stored as `x-amz-meta-*` headers.
    pub metadata: HashMap<String, String>,
}

impl PutOptions {
    /// Creates an empty set of options.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the MIME type explicitly instead of guessing from the key.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }

    /// Sets the `Cache-Control` header.
    #[must_use]
    pub fn cache_control(mut self, value: impl Into<String>) -> Self {
        self.cache_control = Some(value.into());
        self
    }

    /// Sets the `Content-Disposition` header.
    #[must_use]
    pub fn content_disposition(mut self, value: impl Into<String>) -> Self {
        self.content_disposition = Some(value.into());
        self
    }

    /// Sets the `Content-Encoding` header.
    #[must_use]
    pub fn content_encoding(mut self, value: impl Into<String>) -> Self {
        self.content_encoding = Some(value.into());
        self
    }

    /// Sets the `Content-Language` header.
    #[must_use]
    pub fn content_language(mut self, value: impl Into<String>) -> Self {
        self.content_language = Some(value.into());
        self
    }

    /// Adds a single user metadata entry.
    #[must_use]
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Replaces all user metadata at once.
    #[must_use]
    pub fn metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Resolves the content type, falling back to a guess from `key`.
    pub(crate) fn resolved_content_type(&self, key: &str) -> String {
        self.content_type.clone().unwrap_or_else(|| {
            mime_guess::from_path(key)
                .first_or_octet_stream()
                .to_string()
        })
    }
}

/// Which objects to return from a listing.
///
/// ```
/// use cloudflare_r2_rs::ListOptions;
///
/// // Directory-style listing of one "folder".
/// let options = ListOptions::new().prefix("photos/2024/").delimiter("/");
/// assert_eq!(options.delimiter.as_deref(), Some("/"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    /// Only return keys beginning with this prefix.
    pub prefix: Option<String>,
    /// Group keys sharing a path segment; usually `/` for folder-like listings.
    pub delimiter: Option<String>,
    /// Maximum keys per page. R2 caps this at 1000.
    pub max_keys: Option<i32>,
    /// Resume listing after this key.
    pub start_after: Option<String>,
    /// Continuation token from a previous [`ListPage`].
    pub continuation_token: Option<String>,
}

impl ListOptions {
    /// Creates an unfiltered listing request.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts the listing to keys starting with `prefix`.
    #[must_use]
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Groups keys by `delimiter`, returning the groups as common prefixes.
    #[must_use]
    pub fn delimiter(mut self, delimiter: impl Into<String>) -> Self {
        self.delimiter = Some(delimiter.into());
        self
    }

    /// Limits how many keys a single page returns.
    #[must_use]
    pub fn max_keys(mut self, max_keys: i32) -> Self {
        self.max_keys = Some(max_keys);
        self
    }

    /// Starts the listing after the given key.
    #[must_use]
    pub fn start_after(mut self, key: impl Into<String>) -> Self {
        self.start_after = Some(key.into());
        self
    }

    /// Continues a previous listing.
    #[must_use]
    pub fn continuation_token(mut self, token: impl Into<String>) -> Self {
        self.continuation_token = Some(token.into());
        self
    }
}

/// One object as returned by a listing.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ObjectSummary {
    /// The object's key.
    pub key: String,
    /// Size in bytes.
    pub size: i64,
    /// Entity tag, quoted as the service returned it.
    pub etag: Option<String>,
    /// When the object was last written.
    pub last_modified: Option<DateTime>,
}

/// A single page of a listing.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ListPage {
    /// Objects on this page.
    pub objects: Vec<ObjectSummary>,
    /// Grouped prefixes, present only when a delimiter was supplied.
    pub common_prefixes: Vec<String>,
    /// Token to pass to the next call, when [`ListPage::is_truncated`] is set.
    pub next_continuation_token: Option<String>,
    /// Whether more pages remain.
    pub is_truncated: bool,
}

/// Full metadata for a single object, as returned by `HEAD`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ObjectMetadata {
    /// Size in bytes.
    pub content_length: i64,
    /// Stored MIME type.
    pub content_type: Option<String>,
    /// Entity tag.
    pub etag: Option<String>,
    /// When the object was last written.
    pub last_modified: Option<DateTime>,
    /// Stored `Cache-Control` header.
    pub cache_control: Option<String>,
    /// Stored `Content-Disposition` header.
    pub content_disposition: Option<String>,
    /// Stored `Content-Encoding` header.
    pub content_encoding: Option<String>,
    /// Stored `Content-Language` header.
    pub content_language: Option<String>,
    /// User metadata stored with the object.
    pub metadata: HashMap<String, String>,
}

/// Result of storing an object.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PutOutcome {
    /// The key that was written.
    pub key: String,
    /// Entity tag assigned by R2.
    pub etag: Option<String>,
}

/// One key that could not be deleted in a batch delete.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeleteFailure {
    /// The key that survived.
    pub key: String,
    /// Service error code, e.g. `AccessDenied`.
    pub code: Option<String>,
    /// Human-readable reason.
    pub message: Option<String>,
}

/// Outcome of a batch delete. R2 reports per-key failures rather than failing
/// the whole request, so both lists can be non-empty at once.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DeleteReport {
    /// Keys that were removed.
    pub deleted: Vec<String>,
    /// Keys that could not be removed, with the reason.
    pub failed: Vec<DeleteFailure>,
}

impl DeleteReport {
    /// `true` when every requested key was deleted.
    pub fn all_succeeded(&self) -> bool {
        self.failed.is_empty()
    }
}

/// A bucket owned by the account.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BucketSummary {
    /// Bucket name.
    pub name: String,
    /// When the bucket was created.
    pub creation_date: Option<DateTime>,
}

/// Tuning for multipart uploads.
///
/// ```
/// use cloudflare_r2_rs::MultipartOptions;
///
/// let options = MultipartOptions::new().part_size(16 * 1024 * 1024).concurrency(4);
/// assert_eq!(options.concurrency, 4);
/// ```
#[derive(Debug, Clone)]
pub struct MultipartOptions {
    /// Bytes per part. Clamped up to [`MIN_PART_SIZE`], and grown automatically
    /// if the file would otherwise need more than [`MAX_PARTS`] parts.
    pub part_size: u64,
    /// How many parts to upload at once.
    pub concurrency: usize,
    /// Files at or above this size go through multipart; smaller ones are sent
    /// in a single request. Only consulted by
    /// [`upload_file_with`](crate::R2Client::upload_file_with).
    pub threshold: u64,
    /// Headers and user metadata for the finished object.
    pub put_options: PutOptions,
}

impl Default for MultipartOptions {
    fn default() -> Self {
        MultipartOptions {
            part_size: DEFAULT_PART_SIZE,
            concurrency: 8,
            threshold: DEFAULT_MULTIPART_THRESHOLD,
            put_options: PutOptions::default(),
        }
    }
}

impl MultipartOptions {
    /// Creates options with the default part size and concurrency.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the bytes per part.
    #[must_use]
    pub fn part_size(mut self, part_size: u64) -> Self {
        self.part_size = part_size;
        self
    }

    /// Sets how many parts upload concurrently.
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the file size at which `upload_file_with` switches to multipart.
    #[must_use]
    pub fn threshold(mut self, threshold: u64) -> Self {
        self.threshold = threshold;
        self
    }

    /// Sets the headers and user metadata for the finished object.
    #[must_use]
    pub fn put_options(mut self, put_options: PutOptions) -> Self {
        self.put_options = put_options;
        self
    }
}

/// An in-progress multipart upload.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct MultipartUpload {
    /// Key being uploaded to.
    pub key: String,
    /// Upload ID, needed to add parts, complete, or abort.
    pub upload_id: String,
    /// When the upload was started, for listings.
    pub initiated: Option<DateTime>,
}

/// A part that has been uploaded and is ready to be completed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CompletedPart {
    /// 1-based part number.
    pub part_number: i32,
    /// Entity tag returned when the part was uploaded.
    pub etag: String,
}

/// Extra parameters for a presigned URL.
///
/// The `response_*` fields override the stored headers for that one download,
/// which is how you serve a stored object under a different filename.
#[derive(Debug, Clone)]
pub struct PresignOptions {
    /// How long the URL stays valid. R2 allows at most 7 days.
    pub expires_in: Duration,
    /// Overrides `Content-Type` on the response.
    pub response_content_type: Option<String>,
    /// Overrides `Content-Disposition` on the response.
    pub response_content_disposition: Option<String>,
    /// Sets the `Content-Type` the client must send (presigned uploads only).
    pub content_type: Option<String>,
}

impl Default for PresignOptions {
    fn default() -> Self {
        PresignOptions {
            expires_in: Duration::from_secs(3600),
            response_content_type: None,
            response_content_disposition: None,
            content_type: None,
        }
    }
}

impl PresignOptions {
    /// Creates options valid for one hour.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how long the URL stays valid.
    #[must_use]
    pub fn expires_in(mut self, expires_in: Duration) -> Self {
        self.expires_in = expires_in;
        self
    }

    /// Overrides the `Content-Type` served with the download.
    #[must_use]
    pub fn response_content_type(mut self, value: impl Into<String>) -> Self {
        self.response_content_type = Some(value.into());
        self
    }

    /// Overrides the `Content-Disposition` served with the download, e.g.
    /// `attachment; filename="invoice.pdf"`.
    #[must_use]
    pub fn response_content_disposition(mut self, value: impl Into<String>) -> Self {
        self.response_content_disposition = Some(value.into());
        self
    }

    /// Requires the uploading client to send this `Content-Type`.
    #[must_use]
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guesses_content_type_from_key() {
        let options = PutOptions::new();
        assert_eq!(options.resolved_content_type("a/b/photo.png"), "image/png");
    }

    #[test]
    fn explicit_content_type_wins_over_guess() {
        let options = PutOptions::new().content_type("text/plain");
        assert_eq!(options.resolved_content_type("photo.png"), "text/plain");
    }

    #[test]
    fn unknown_extension_falls_back_to_octet_stream() {
        let options = PutOptions::new();
        assert_eq!(
            options.resolved_content_type("blob.zzzz"),
            "application/octet-stream"
        );
    }

    #[test]
    fn delete_report_tracks_partial_failure() {
        let report = DeleteReport {
            deleted: vec!["a".into()],
            failed: vec![DeleteFailure {
                key: "b".into(),
                code: Some("AccessDenied".into()),
                message: None,
            }],
        };
        assert!(!report.all_succeeded());
    }
}
