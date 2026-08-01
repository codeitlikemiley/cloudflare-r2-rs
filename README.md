# cloudflare-r2-rs

A fully featured, ergonomic Rust SDK for [Cloudflare R2](https://developers.cloudflare.com/r2/).

R2 speaks the S3 protocol, so this crate is a thin, opinionated layer over
`aws-sdk-s3` that handles the R2-specific parts for you — endpoint construction
from an account ID, jurisdiction endpoints, path-style addressing, and the
`auto` signing region. What you get is a bucket-bound client with typed errors
and no builder ceremony at the call site.

```toml
[dependencies]
cloudflare-r2-rs = "0.2"
tokio = { version = "1", features = ["full"] }
```

## Quick start

```rust,no_run
use cloudflare_r2_rs::{R2Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = R2Client::builder()
        .account_id("0123456789abcdef")
        .bucket("media")
        .access_key_id("access-key")
        .secret_access_key("secret-key")
        .build()?;

    client.put_object("hello.txt", "hello world").await?;
    let body = client.get_object("hello.txt").await?;
    assert_eq!(body, b"hello world");
    Ok(())
}
```

The builder is a typestate: `build()` does not exist until the endpoint,
bucket, access key and secret key have all been set, so a half-configured
client is a compile error rather than a runtime one.

### From the environment

```rust
use cloudflare_r2_rs::{R2Client, Result};

fn connect() -> Result<R2Client> {
    R2Client::from_env()
}
```

| Variable | Required | Legacy fallback | Notes |
| --- | --- | --- | --- |
| `R2_ACCESS_KEY_ID` | yes | `CLOUDFLARE_CLIENT_ID` | |
| `R2_SECRET_ACCESS_KEY` | yes | `CLOUDFLARE_SECRET_KEY` | |
| `R2_BUCKET` | yes | `CLOUDFLARE_BUCKET_NAME` | |
| `R2_ENDPOINT` | see below | `CLOUDFLARE_URL` | Used verbatim when set |
| `R2_ACCOUNT_ID` | see below | `CLOUDFLARE_ACCOUNT_ID` | Endpoint is derived from it |
| `R2_JURISDICTION` | no | — | `default`, `eu` or `fedramp` |
| `R2_REGION` | no | — | Defaults to `auto` |

Set either `R2_ENDPOINT` or `R2_ACCOUNT_ID`. `R2_ENDPOINT` wins if both are
present, in which case `R2_ACCOUNT_ID` and `R2_JURISDICTION` are ignored. A
blank value counts as unset.

## What's covered

### Objects

```rust
use cloudflare_r2_rs::{PutOptions, R2Client, Result};

async fn objects(client: &R2Client, bytes: Vec<u8>) -> Result<()> {
    // Bodies can be a &str, String, Vec<u8>, byte slice or array, Bytes, or a
    // ByteStream. Content type is guessed from the key unless you set it.
    client.put_object("notes/hello.txt", "hello world").await?;
    client.put_object("photos/cat.png", bytes.clone()).await?;

    client.put_object_with(
        "reports/q3.pdf",
        bytes,
        PutOptions::new()
            .cache_control("public, max-age=86400")
            .content_disposition("attachment; filename=\"q3.pdf\"")
            .metadata("generated-by", "billing-service"),
    ).await?;

    let _body     = client.get_object("photos/cat.png").await?;
    let _first_kb = client.get_object_range("photos/cat.png", 0, Some(1023)).await?;
    let _stream   = client.get_object_stream("photos/cat.png").await?; // no buffering
    let _meta     = client.head_object("photos/cat.png").await?;
    let _exists   = client.object_exists("photos/cat.png").await?;

    client.copy_object("photos/cat.png", "archive/cat.png").await?;
    client.delete_object("photos/cat.png").await?;
    Ok(())
}
```

### Listing

```rust
use cloudflare_r2_rs::{ListOptions, R2Client, Result};

async fn listing(client: &R2Client) -> Result<()> {
    // One page at a time.
    let page = client
        .list_objects(ListOptions::new().prefix("photos/").max_keys(100))
        .await?;
    for object in &page.objects {
        println!("{} ({} bytes)", object.key, object.size);
    }

    // Or let the crate follow pagination for you.
    let _everything = client.list_all_objects(Some("photos/")).await?;

    // Folder-style browsing.
    let _folders = client.list_prefixes("photos/").await?;
    Ok(())
}
```

### Files and multipart

```rust
use std::path::Path;

use cloudflare_r2_rs::{MultipartOptions, R2Client, Result};

async fn files(client: &R2Client) -> Result<()> {
    // Streams from disk, and switches to multipart above 16 MiB automatically.
    client.upload_file("videos/clip.mp4", Path::new("clip.mp4")).await?;

    // Or drive it yourself, including where the multipart cutoff sits.
    client.upload_file_with(
        "videos/clip.mp4",
        Path::new("clip.mp4"),
        MultipartOptions::new()
            .part_size(16 * 1024 * 1024)
            .concurrency(8)
            .threshold(32 * 1024 * 1024),
    ).await?;

    client.download_to("videos/clip.mp4", Path::new("/tmp/clip.mp4")).await?;
    Ok(())
}
```

Part size is clamped up to R2's 5 MiB minimum and grown automatically if the
file would otherwise need more than 10,000 parts. Every part is the same size
except the last, which is what R2 requires — S3 only requires a minimum.

If a part fails, the upload is aborted on a best-effort basis. Should the abort
itself fail, or the process die mid-upload, the incomplete upload keeps holding
storage: `list_multipart_uploads()` finds those, and a lifecycle rule can expire
them automatically.

The low-level `create_multipart_upload` / `upload_part` /
`complete_multipart_upload` / `abort_multipart_upload` calls are public too,
and `list_multipart_uploads` finds uploads that were interrupted.

### Presigned URLs

```rust
use std::time::Duration;

use cloudflare_r2_rs::{PresignOptions, R2Client, Result};

async fn signed(client: &R2Client) -> Result<()> {
    // Let a browser upload directly.
    let _upload = client
        .presign_put("uploads/avatar.png", Duration::from_secs(900))
        .await?;

    // Serve a stored object under a different filename.
    let download = client.presign_get_with(
        "reports/q3.pdf",
        PresignOptions::new()
            .expires_in(Duration::from_secs(300))
            .response_content_disposition("attachment; filename=\"invoice.pdf\""),
    ).await?;
    println!("{}", download.url);
    Ok(())
}
```

Any headers listed on the returned `PresignedRequest` were part of the
signature and the client must send them verbatim. Expiry is capped at seven
days, which R2 enforces anyway — this crate rejects it up front rather than
letting you hand out a URL that fails.

### Batch deletes

```rust
use cloudflare_r2_rs::{R2Client, Result};

async fn deletes(client: &R2Client) -> Result<()> {
    let report = client.delete_objects(vec!["a.txt", "b.txt"]).await?;
    if !report.all_succeeded() {
        for failure in &report.failed {
            eprintln!("{}: {:?}", failure.key, failure.message);
        }
    }

    // Everything under a prefix. An empty prefix is refused.
    client.delete_prefix("tmp/").await?;
    Ok(())
}
```

R2 reports per-key failures rather than failing the whole request, so
`DeleteReport` carries both lists. Requests are batched at 1000 keys.

### Buckets

```rust
use cloudflare_r2_rs::{R2Client, Result};

async fn buckets(client: &R2Client) -> Result<()> {
    client.create_bucket().await?;
    let _exists  = client.bucket_exists().await?;
    let _buckets = client.list_buckets().await?;

    let _cors  = client.get_cors().await?;      // empty vec when unset
    let _rules = client.get_lifecycle().await?; // empty vec when unset

    // Same credentials and connection pool, different bucket.
    let _other = client.with_bucket("thumbnails");
    Ok(())
}
```

CORS and lifecycle take the `aws-sdk-s3` rule types directly, re-exported
under `cloudflare_r2_rs::s3`, rather than being mirrored.

## Errors

Every operation returns `Result<T, Error>` with a typed error. Missing objects
surface as `Error::ObjectNotFound` rather than an opaque service error:

```rust
use cloudflare_r2_rs::{R2Client, Result};

async fn read(client: &R2Client) -> Result<()> {
    match client.get_object("maybe.txt").await {
        Ok(body) => println!("{} bytes", body.len()),
        Err(err) if err.is_not_found() => println!("not there"),
        Err(err) => return Err(err),
    }
    Ok(())
}
```

`Error::status()` exposes the HTTP status when the failure reached the
service, and `Error::Api` keeps the underlying SDK error as its `source` so it
can be downcast to the typed service error.

Note that `is_not_found()` is not true for a `403`. With a bucket-scoped R2 API
token, "exists but this token cannot see it" is a different answer from "does
not exist", and the crate does not conflate them — `bucket_exists()` and
`object_exists()` return an error rather than `false` in that case.

## Escape hatch

Anything this crate does not wrap is reachable through `client.inner()`, which
hands back the underlying `aws_sdk_s3::Client` configured for R2.

## Safety rails

A few things this crate refuses rather than passing through:

- Keys that would escape the destination directory in `download_file` —
  absolute paths, `..` segments, Windows drive prefixes, backslashes. These are
  legal R2 keys, so they are refused only where a key becomes a filesystem
  path; every network operation still accepts them, and `download_to` will
  fetch one to a destination you choose.
- `delete_prefix("")`, which would silently empty the bucket.
- Presign expiries of zero or over seven days.
- Multipart uploads of empty files, and zero concurrency.
- An invalid key anywhere in a `delete_objects` batch, checked before the first
  request, so a bad key never leaves a half-applied delete behind.

Two more things it handles quietly:

- **Downloads land atomically.** `download_to` streams to a temporary file
  beside the destination and renames it into place only once the whole body has
  arrived, so an interrupted transfer never leaves a truncated file where a
  complete one is expected.
- **Uploads are not `aws-chunked`.** Recent `aws-sdk-s3` releases default to
  sending streamed uploads with a trailing checksum and a
  `Content-Encoding: aws-chunked` header. R2 stores that header on the object
  instead of stripping it, which leaves the object undecodable to browsers, so
  this crate configures checksums to be sent only where the operation requires
  them.

## Examples

```sh
cargo run --example basic       # objects, listing, metadata, cleanup
cargo run --example multipart   # concurrent large-file upload
cargo run --example presigned   # signed upload and download URLs
```

Each reads credentials from the environment (a `.env` file works — see
`.env.example`).

## Testing

```sh
cargo test                                              # offline: no network
cargo test --test live -- --ignored --test-threads=1    # against a real bucket
```

The offline suite includes `tests/offline_http.rs`, which runs the client
against a throwaway HTTP server on localhost. That covers the wire format,
pagination, batching, XML parsing and error mapping without credentials or
network access.

The live tests namespace every key under `cloudflare-r2-rs-tests/` and clean up
after themselves, but point them at a disposable bucket.

Every Rust snippet in this file is compiled as part of the doctest suite, so the
code above cannot drift from the API. The `toml` and `sh` blocks are not
checked — the dependency version in particular is maintained by hand.

## Minimum supported Rust version

1.94.1, as required by `aws-sdk-s3`.

## License

[MIT](LICENSE).
