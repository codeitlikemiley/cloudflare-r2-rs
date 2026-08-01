# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0]

Complete redesign of the crate into a full-coverage R2 SDK. This release is not
backwards compatible with `0.1.0`; see **Migrating** below.

### Added

- **Configuration** — `R2Config`, `R2Client::from_env`, endpoint derivation from
  a Cloudflare account ID, and `Jurisdiction` support for the `eu` and
  `fedramp` endpoints.
- **Typed errors** — the `Error` enum replaces `anyhow`. Missing objects surface
  as `Error::ObjectNotFound`, with `Error::is_not_found` and `Error::status`
  for the general case.
- **Object operations** — `put_object_with`, `get_object_range`,
  `get_object_stream`, `head_object`, `object_exists`, `copy_object`,
  `copy_object_from`, `delete_objects` (batched at 1000 keys, with per-key
  failure reporting) and `delete_prefix`.
- **Listing** — `list_objects` for a single page with prefix, delimiter,
  `max_keys`, `start_after` and continuation tokens; `list_all_objects` to
  follow pagination; `list_prefixes` for folder-style browsing.
- **File transfer** — `upload_file` streams from disk and switches to multipart
  above 16 MiB automatically; `download_to` streams back to an exact path.
- **Multipart uploads** — `multipart_upload_file` with configurable part size
  and concurrency, aborting the upload if any part fails, plus the low-level
  `create_multipart_upload`, `upload_part`, `complete_multipart_upload`,
  `abort_multipart_upload` and `list_multipart_uploads`.
- **Presigned URLs** — `presign_get`, `presign_put`, `presign_delete` and
  `presign_head`, with response-header overrides and a seven-day expiry cap.
- **Bucket operations** — `bucket_exists`, `list_buckets`, `with_bucket`, and
  CORS and lifecycle configuration.
- **Escape hatch** — `R2Client::inner` exposes the underlying `aws-sdk-s3`
  client.
- Runnable `examples/`, a `.env.example`, GitHub Actions CI, and an offline
  test suite plus `#[ignore]`d live tests.

### Changed

- `R2Manager`/`CloudFlareR2` is now `R2Client`, and the builder is
  `R2ClientBuilder`. The typestate guarantee is retained: `build()` does not
  exist until every required field is set.
- Requests use path-style addressing, which is what R2's S3-compatible
  endpoint expects, and sign with the `auto` region rather than `us-east-1`.
- `put_object` returns a `PutOutcome` (key and ETag) instead of the key.
- `delete_object` returns `()` instead of `bool`; deleting a key that does not
  exist succeeds, matching S3 semantics.
- `download_file` returns a `PathBuf` and streams asynchronously rather than
  blocking the runtime on synchronous file writes.
- `list_keys` no longer loops forever on a truncated page that returns no
  continuation token.

### Security

- Keys containing `..` path segments are rejected, so a hostile key cannot
  escape the target directory during `download_file`.
- `delete_prefix("")` is refused rather than silently emptying the bucket.

### Migrating from 0.1

| 0.1 | 0.2 |
| --- | --- |
| `CloudFlareR2::builder()` | `R2Client::builder()` |
| `.bucket_name(x)` | `.bucket(x)` |
| `.client_id(x)` | `.access_key_id(x)` |
| `.secret_key(x)` | `.secret_access_key(x)` |
| `.url(x)` | `.endpoint(x)`, or `.account_id(x)` |
| `get_bucket_name()` | `bucket()` |
| `put_object(k, v) -> String` | `put_object(k, v) -> PutOutcome` |
| `delete_object(k) -> bool` | `delete_object(k) -> ()` |
| `download_file(k, dir) -> String` | `download_file(k, dir) -> PathBuf` |
| `anyhow::Result<T>` | `cloudflare_r2_rs::Result<T>` |

## [0.1.0]

- Initial release: typestate builder, bucket create/delete, and
  put/get/delete/download/list of objects.
