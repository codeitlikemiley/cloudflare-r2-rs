//! A fully featured, ergonomic Rust SDK for [Cloudflare R2](https://developers.cloudflare.com/r2/).
//!
//! R2 speaks the S3 protocol, so this crate is a thin, opinionated layer over
//! `aws-sdk-s3` that handles the R2-specific parts for you: endpoint
//! construction from an account ID, jurisdiction endpoints, path-style
//! addressing, and the `auto` signing region. What you get back is a
//! bucket-bound client with typed errors and no builder ceremony at the call
//! site.
//!
//! # Quick start
//!
//! ```no_run
//! use cloudflare_r2_rs::{R2Client, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = R2Client::builder()
//!         .account_id("0123456789abcdef")
//!         .bucket("media")
//!         .access_key_id("access-key")
//!         .secret_access_key("secret-key")
//!         .build()?;
//!
//!     client.put_object("hello.txt", "hello world").await?;
//!     let body = client.get_object("hello.txt").await?;
//!     assert_eq!(body, b"hello world");
//!     Ok(())
//! }
//! ```
//!
//! Or read the same settings from the environment with
//! [`R2Client::from_env`], which understands `R2_ACCOUNT_ID`,
//! `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` and `R2_BUCKET`.
//!
//! # What's here
//!
//! - **Objects** — [`put_object`](R2Client::put_object),
//!   [`get_object`](R2Client::get_object),
//!   [`get_object_range`](R2Client::get_object_range),
//!   [`head_object`](R2Client::head_object),
//!   [`copy_object`](R2Client::copy_object),
//!   [`delete_object`](R2Client::delete_object),
//!   [`delete_objects`](R2Client::delete_objects),
//!   [`delete_prefix`](R2Client::delete_prefix).
//! - **Listing** — [`list_objects`](R2Client::list_objects) for one page,
//!   [`list_all_objects`](R2Client::list_all_objects) to follow pagination, and
//!   [`list_prefixes`](R2Client::list_prefixes) for folder-style browsing.
//! - **Files** — [`upload_file`](R2Client::upload_file) streams from disk and
//!   switches to multipart automatically;
//!   [`download_to`](R2Client::download_to) streams back.
//! - **Multipart** — [`multipart_upload_file`](R2Client::multipart_upload_file)
//!   for concurrent parts, plus the low-level
//!   [`create_multipart_upload`](R2Client::create_multipart_upload) /
//!   [`upload_part`](R2Client::upload_part) /
//!   [`complete_multipart_upload`](R2Client::complete_multipart_upload) trio.
//! - **Presigned URLs** — [`presign_get`](R2Client::presign_get),
//!   [`presign_put`](R2Client::presign_put),
//!   [`presign_delete`](R2Client::presign_delete).
//! - **Buckets** — create, delete, existence checks, listing, plus CORS and
//!   lifecycle configuration.
//!
//! # Errors
//!
//! Every operation returns [`Result<T>`], whose error is the typed [`Error`]
//! enum. Missing objects surface as [`Error::ObjectNotFound`] rather than an
//! opaque service error, and [`Error::is_not_found`] covers the general case:
//!
//! ```no_run
//! # async fn run(client: cloudflare_r2_rs::R2Client) -> cloudflare_r2_rs::Result<()> {
//! match client.get_object("maybe.txt").await {
//!     Ok(body) => println!("{} bytes", body.len()),
//!     Err(err) if err.is_not_found() => println!("not there"),
//!     Err(err) => return Err(err),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Escape hatch
//!
//! Anything this crate does not wrap is still reachable through
//! [`R2Client::inner`], which hands back the underlying `aws-sdk-s3` client.

#![warn(missing_docs)]
#![warn(clippy::all)]

mod body;
mod client;
mod config;
mod error;
mod multipart;
mod object;
mod presign;
mod types;

pub use body::IntoBody;
pub use client::{
    HasAccessKey, HasBucket, HasEndpoint, HasSecretKey, NoAccessKey, NoBucket, NoEndpoint,
    NoSecretKey, R2Client, R2ClientBuilder,
};
pub use config::{endpoint_for_account, Jurisdiction, R2Config, DEFAULT_REGION};
pub use error::{Error, Result};
pub use presign::{PresignedRequest, MAX_PRESIGN_EXPIRY};
pub use types::{
    BucketSummary, CompletedPart, DateTime, DeleteFailure, DeleteReport, ListOptions, ListPage,
    MultipartOptions, MultipartUpload, ObjectMetadata, ObjectSummary, PresignOptions, PutOptions,
    PutOutcome, DEFAULT_MULTIPART_THRESHOLD, DEFAULT_PART_SIZE, MAX_DELETE_BATCH, MAX_PARTS,
    MIN_PART_SIZE,
};

/// Re-exported `aws-sdk-s3` items that appear in this crate's public API.
///
/// CORS and lifecycle rules are deep, rarely-touched structures, so rather than
/// mirror them this crate takes the SDK's own types directly.
pub mod s3 {
    pub use aws_sdk_s3::config::retry::RetryConfig;
    pub use aws_sdk_s3::config::timeout::TimeoutConfig;
    pub use aws_sdk_s3::primitives::ByteStream;
    pub use aws_sdk_s3::types::{
        CorsRule, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
        NoncurrentVersionExpiration,
    };
    pub use aws_sdk_s3::Client;
}

/// Compiles every code block in `README.md` as a doctest, so the examples
/// there cannot drift out of sync with the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeExamples;
