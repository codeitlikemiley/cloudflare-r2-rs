//! Offline tests exercising the crate through its public API only.
//!
//! These make no network calls: they cover configuration, argument validation
//! and URL signing, all of which happen before any request is sent. For tests
//! that talk to a real bucket see `tests/live.rs`.

use std::time::Duration;

use cloudflare_r2_rs::{
    endpoint_for_account, Error, Jurisdiction, ListOptions, MultipartOptions, PresignOptions,
    PutOptions, R2Client, R2Config, MAX_PRESIGN_EXPIRY,
};

fn client() -> R2Client {
    R2Client::builder()
        .account_id("0123456789abcdef")
        .bucket("test-bucket")
        .access_key_id("test-access-key")
        .secret_access_key("test-secret-key")
        .build()
        .expect("client should build")
}

#[test]
fn builder_derives_the_r2_endpoint() {
    assert_eq!(
        client().endpoint(),
        "https://0123456789abcdef.r2.cloudflarestorage.com"
    );
}

#[test]
fn builder_supports_every_jurisdiction() {
    for (jurisdiction, expected) in [
        (
            Jurisdiction::Default,
            "https://acct.r2.cloudflarestorage.com",
        ),
        (Jurisdiction::Eu, "https://acct.eu.r2.cloudflarestorage.com"),
        (
            Jurisdiction::FedRamp,
            "https://acct.fedramp.r2.cloudflarestorage.com",
        ),
    ] {
        assert_eq!(endpoint_for_account("acct", jurisdiction), expected);
    }
}

#[test]
fn from_config_rejects_blank_bucket() {
    let err = R2Client::from_config(R2Config {
        endpoint: "https://acct.r2.cloudflarestorage.com".into(),
        access_key_id: "key".into(),
        secret_access_key: "secret".into(),
        bucket: "".into(),
        region: "auto".into(),
    })
    .unwrap_err();
    assert!(matches!(err, Error::MissingConfig("bucket")));
}

#[test]
fn trailing_slash_is_trimmed_from_the_endpoint() {
    let client = R2Client::from_config(R2Config {
        endpoint: "https://acct.r2.cloudflarestorage.com/".into(),
        access_key_id: "key".into(),
        secret_access_key: "secret".into(),
        bucket: "b".into(),
        region: "auto".into(),
    })
    .unwrap();
    assert_eq!(client.endpoint(), "https://acct.r2.cloudflarestorage.com");
}

#[tokio::test]
async fn a_trimmed_endpoint_does_not_produce_a_doubled_slash_when_signing() {
    let client = R2Client::from_config(R2Config {
        endpoint: "https://acct.r2.cloudflarestorage.com/".into(),
        access_key_id: "key".into(),
        secret_access_key: "secret".into(),
        bucket: "b".into(),
        region: "auto".into(),
    })
    .unwrap();

    let url = client
        .presign_get("a.txt", Duration::from_secs(60))
        .await
        .unwrap()
        .into_url();
    assert!(
        url.starts_with("https://acct.r2.cloudflarestorage.com/b/a.txt?"),
        "{url}"
    );
}

#[tokio::test]
async fn signed_urls_are_path_style_and_carry_a_signature() {
    let request = client()
        .presign_get("photos/cat.png", Duration::from_secs(900))
        .await
        .unwrap();

    assert_eq!(request.method, "GET");
    assert!(request.url.starts_with(
        "https://0123456789abcdef.r2.cloudflarestorage.com/test-bucket/photos/cat.png?"
    ));
    assert!(request.url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
    assert!(request.url.contains("X-Amz-Signature="));
    assert!(request.url.contains("X-Amz-Expires=900"));
}

#[tokio::test]
async fn signed_upload_url_pins_the_content_type() {
    let request = client()
        .presign_put_with(
            "uploads/avatar.png",
            PresignOptions::new()
                .expires_in(Duration::from_secs(60))
                .content_type("image/png"),
        )
        .await
        .unwrap();

    assert_eq!(request.method, "PUT");
    let signed_content_type = request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.clone());
    assert_eq!(signed_content_type.as_deref(), Some("image/png"));
}

#[tokio::test]
async fn presign_expiry_is_bounded_at_seven_days() {
    let client = client();
    assert!(client
        .presign_get("a.txt", MAX_PRESIGN_EXPIRY)
        .await
        .is_ok());
    assert!(client
        .presign_get("a.txt", MAX_PRESIGN_EXPIRY + Duration::from_secs(1))
        .await
        .is_err());
}

#[tokio::test]
async fn unusable_keys_are_refused_before_any_request() {
    let client = client();
    for key in ["", "with\0nul"] {
        let err = client.head_object(key).await.unwrap_err();
        assert!(
            matches!(err, Error::InvalidArgument { .. }),
            "key {key:?} should be rejected before any request"
        );
    }
}

#[tokio::test]
async fn keys_that_look_like_traversal_are_still_valid_object_keys() {
    // `..` and leading slashes are legal in R2. Rejecting them on network
    // operations would make real objects permanently unreachable; only the
    // filesystem-facing download path needs to care.
    let client = client();
    for key in ["../secrets.txt", "/leading.txt", "a/../b.txt"] {
        let signed = client.presign_get(key, Duration::from_secs(60)).await;
        assert!(signed.is_ok(), "key {key:?} should be usable remotely");
    }
}

#[tokio::test]
async fn range_end_before_start_is_refused() {
    let err = client()
        .get_object_range("a.txt", 100, Some(10))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidArgument {
            argument: "end",
            ..
        }
    ));
}

#[tokio::test]
async fn delete_prefix_refuses_an_empty_prefix() {
    let err = client().delete_prefix("").await.unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidArgument {
            argument: "prefix",
            ..
        }
    ));
}

#[tokio::test]
async fn deleting_no_keys_is_a_no_op_that_makes_no_request() {
    let report = client()
        .delete_objects(Vec::<String>::new())
        .await
        .expect("empty batch should not reach the network");
    assert!(report.all_succeeded());
    assert!(report.deleted.is_empty());
}

#[tokio::test]
async fn put_object_accepts_the_obvious_body_types() {
    // A compile-level check: these all have to work without the caller
    // reaching for Vec<u8> or ByteStream.
    fn assert_body(_: impl cloudflare_r2_rs::IntoBody) {}
    assert_body("a str");
    assert_body(String::from("a String"));
    assert_body(b"a byte literal");
    assert_body(vec![1u8, 2, 3]);
    assert_body([1u8, 2, 3].as_slice());
    assert_body(bytes::Bytes::from_static(b"bytes"));
}

#[tokio::test]
async fn multipart_rejects_zero_concurrency() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let err = client()
        .multipart_upload_file(
            "big.bin",
            file.path(),
            MultipartOptions::new().concurrency(0),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidArgument {
            argument: "concurrency",
            ..
        }
    ));
}

#[tokio::test]
async fn multipart_rejects_an_empty_file() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let err = client()
        .multipart_upload_file("empty.bin", file.path(), MultipartOptions::new())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Multipart { .. }));
}

#[tokio::test]
async fn upload_file_reports_a_missing_local_file_with_its_path() {
    let err = client()
        .upload_file("x.bin", std::path::Path::new("/definitely/not/here.bin"))
        .await
        .unwrap_err();
    // The path has to be in the message, or the caller cannot tell which file
    // was missing.
    assert!(matches!(err, Error::File { .. }), "{err:?}");
    assert!(err.to_string().contains("/definitely/not/here.bin"));
}

#[tokio::test]
async fn download_file_requires_an_existing_directory() {
    let err = client()
        .download_file("a.txt", std::path::Path::new("/definitely/not/a/dir"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        Error::InvalidArgument {
            argument: "directory",
            ..
        }
    ));
}

#[test]
fn option_builders_are_chainable() {
    let put = PutOptions::new()
        .content_type("application/json")
        .cache_control("no-store")
        .metadata("k", "v");
    assert_eq!(put.content_type.as_deref(), Some("application/json"));
    assert_eq!(put.cache_control.as_deref(), Some("no-store"));
    assert_eq!(put.metadata.get("k").map(String::as_str), Some("v"));

    let list = ListOptions::new().prefix("a/").delimiter("/").max_keys(10);
    assert_eq!(list.prefix.as_deref(), Some("a/"));
    assert_eq!(list.max_keys, Some(10));
}

#[test]
fn not_found_errors_report_a_404_status() {
    let err = Error::ObjectNotFound {
        bucket: "b".into(),
        key: "k".into(),
    };
    assert!(err.is_not_found());
    assert_eq!(err.status(), Some(404));
    assert!(err.to_string().contains('k'));
}
