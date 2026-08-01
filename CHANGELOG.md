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
- **Body ergonomics** — `IntoBody` lets `put_object` and `upload_part` accept
  `&str`, `String`, `&[u8]`, byte arrays, `Vec<u8>`, `Bytes` or a `ByteStream`,
  rather than only the three types `ByteStream` converts from.
- **Tuning** — `R2ClientBuilder::retry_config` and `timeout_config`, and a
  configurable `MultipartOptions::threshold` for the single-shot/multipart
  cutoff.
- Runnable `examples/`, a `.env.example`, an offline test suite — including
  `tests/offline_http.rs`, which runs the client against a local mock endpoint
  to cover wire format, pagination, batching and error mapping — plus
  `#[ignore]`d live tests. A GitHub Actions workflow is included at
  `.github/workflows-disabled/ci.yml`; move it to `.github/workflows/` to
  activate it.

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
  continuation token; `list_multipart_uploads` now paginates too, rather than
  silently returning only the first 1000 in-progress uploads.
- Checksums are configured as "when required". Left at the SDK default, every
  streamed upload is framed as `aws-chunked` with a trailing CRC32; R2 stores
  that framing header on the object instead of stripping it, leaving the object
  with `Content-Encoding: aws-chunked` and undecodable to browsers. Operations
  that require a checksum, such as `DeleteObjects`, are unaffected.
- Retry and timeout policies are set explicitly (standard retries, 5s connect
  timeout, no operation timeout) rather than inherited from a default that
  shifts with the SDK's behavior version.
- The builder resolves the endpoint at `build()` time, so `jurisdiction()` has
  the same effect before or after `account_id()`. An explicit `endpoint()` wins
  over a derived one in either order.
- `get_cors` and `get_lifecycle` return an empty vector only for the specific
  "no configuration" error. A missing bucket is now `Error::BucketNotFound`
  instead of being reported as "no rules configured".
- `bucket_exists` and `object_exists` return an error rather than `false` on a
  `403`, since a scoped API token cannot distinguish "hidden" from "absent".
- `delete_objects` validates every key before the first request, so an invalid
  key cannot leave earlier batches already deleted.
- `download_to` writes to a temporary file and renames it into place, so an
  interrupted download never leaves a truncated file at the destination.
- Local file failures during upload are `Error::File`, carrying the path, rather
  than `Error::Body` ("failed to read response body").
- `Error::Api` now carries the most specific message from the SDK error chain
  rather than a concatenation of every level, which had duplicated the chain for
  anything printing `source`.
- `R2Config`'s `Debug` redacts the secret access key.
- The minimum supported Rust version is 1.94.1, required by `aws-sdk-s3`.
  `0.1.0` declared no `rust-version` at all.
- Output structs (`ObjectSummary`, `ObjectMetadata`, `ListPage`, `PutOutcome`,
  `DeleteReport`, `DeleteFailure`, `BucketSummary`, `MultipartUpload`,
  `CompletedPart`) are `#[non_exhaustive]`, so future fields are not breaking
  changes. Options structs stay constructible, since callers build those.

### Security

- `download_file` refuses any key that would resolve outside the destination
  directory: absolute keys, `..` segments, Windows drive prefixes and
  backslashes. Previously only `..` was checked, so a leading `/` made
  `Path::join` discard the destination directory entirely.
- That check applies only where a key becomes a filesystem path. Such keys are
  legal in R2, so every network operation still accepts them and `download_to`
  can fetch one to an explicit destination — an earlier iteration rejected them
  everywhere, which made real objects unreachable.
- `delete_prefix("")` is refused rather than silently emptying the bucket.
- `R2Config` no longer prints the secret access key through `Debug`.

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
| `put_object(k, bytes.to_vec())` | `put_object(k, bytes)` — any `IntoBody` |

## [0.1.0]

- Initial release: typestate builder, bucket create/delete, and
  put/get/delete/download/list of objects.
