//! Offline end-to-end tests against a mock S3 endpoint.
//!
//! The builder accepts any endpoint URL, so a throwaway HTTP server on
//! localhost exercises everything that happens *after* a request is built —
//! wire format, pagination, batching, XML parsing and error mapping — with no
//! network access and no credentials. These are the tests that catch the
//! defects unit tests structurally cannot see.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cloudflare_r2_rs::{Error, MultipartOptions, PutOptions, R2Client};
use tokio::net::{TcpListener, TcpStream};

/// One request the mock server received.
#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    /// All values for a header, lowercased name, in the order received.
    fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
            .collect()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.header_values(name).first().copied()
    }
}

/// A canned response to hand back.
#[derive(Clone)]
struct Canned {
    status: u16,
    body: String,
    /// When set, `Content-Length` advertises this many bytes but only `body` is
    /// actually written before the socket closes — the shape of a connection
    /// dropped mid-transfer.
    declared_length: Option<usize>,
}

impl Canned {
    fn ok(body: impl Into<String>) -> Self {
        Canned {
            status: 200,
            body: body.into(),
            declared_length: None,
        }
    }

    /// A response that promises `declared` bytes, sends `body`, then hangs up.
    fn truncated(body: impl Into<String>, declared: usize) -> Self {
        Canned {
            status: 200,
            body: body.into(),
            declared_length: Some(declared),
        }
    }

    fn error(status: u16, code: &str, message: &str) -> Self {
        Canned {
            status,
            body: format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>{code}</Code><Message>{message}</Message></Error>"#
            ),
            declared_length: None,
        }
    }
}

/// A minimal S3-shaped HTTP server that records what it is sent.
struct MockR2 {
    endpoint: String,
    recorded: Arc<Mutex<Vec<Recorded>>>,
}

impl MockR2 {
    /// Starts a server that replies with `responses` in order, then with an
    /// empty `200` once they run out.
    async fn start(responses: Vec<Canned>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let queue = Arc::new(Mutex::new(VecDeque::from(responses)));

        let accepted = Arc::clone(&recorded);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&accepted);
                let queue = Arc::clone(&queue);
                tokio::spawn(async move {
                    // One request per connection: every response says
                    // `Connection: close`, so the client reconnects each time.
                    if let Some(request) = read_request(&stream).await {
                        recorded.lock().unwrap().push(request);
                        let canned = queue
                            .lock()
                            .unwrap()
                            .pop_front()
                            .unwrap_or_else(|| Canned::ok(""));
                        let _ = write_response(&stream, &canned).await;
                    }
                });
            }
        });

        MockR2 {
            endpoint: format!("http://{address}"),
            recorded,
        }
    }

    fn client(&self) -> R2Client {
        R2Client::builder()
            .endpoint(&self.endpoint)
            .bucket("test-bucket")
            .access_key_id("test-key")
            .secret_access_key("test-secret")
            // Keep failures fast and deterministic: a retry would double the
            // recorded requests and confuse the assertions below.
            .retry_config(cloudflare_r2_rs::s3::RetryConfig::disabled())
            .build()
            .unwrap()
    }

    fn requests(&self) -> Vec<Recorded> {
        self.recorded.lock().unwrap().clone()
    }

    fn first(&self) -> Recorded {
        self.requests()
            .first()
            .cloned()
            .expect("no request arrived")
    }
}

/// Reads one HTTP/1.1 request, honouring both framing styles the SDK uses.
async fn read_request(stream: &TcpStream) -> Option<Recorded> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];

    // Headers first.
    let header_end = loop {
        if let Some(position) = find(&buffer, b"\r\n\r\n") {
            break position;
        }
        stream.readable().await.ok()?;
        match stream.try_read(&mut chunk) {
            Ok(0) => return None,
            Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => return None,
        }
    };

    let head = String::from_utf8_lossy(&buffer[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();

    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }

    let mut body = buffer[header_end + 4..].to_vec();

    let content_length: Option<usize> = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.parse().ok());
    let chunked = headers
        .iter()
        .any(|(name, value)| name == "transfer-encoding" && value.contains("chunked"));

    if let Some(length) = content_length {
        while body.len() < length {
            if !read_more(stream, &mut body).await {
                break;
            }
        }
        body.truncate(length.min(body.len()));
    } else if chunked {
        // Decode the chunk framing rather than scanning for a terminator byte
        // sequence: payload data can contain "\r\n0\r\n", which would end the
        // read early and silently drop the rest of the request.
        body = decode_chunked(stream, body).await;
    }

    Some(Recorded {
        method,
        target,
        headers,
        body,
    })
}

/// Pulls one more read's worth of bytes into `buffer`. Returns false at EOF.
async fn read_more(stream: &TcpStream, buffer: &mut Vec<u8>) -> bool {
    let mut chunk = [0u8; 4096];
    loop {
        if stream.readable().await.is_err() {
            return false;
        }
        match stream.try_read(&mut chunk) {
            Ok(0) => return false,
            Ok(read) => {
                buffer.extend_from_slice(&chunk[..read]);
                return true;
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => return false,
        }
    }
}

/// Decodes an HTTP/1.1 chunked body, returning just the payload.
async fn decode_chunked(stream: &TcpStream, initial: Vec<u8>) -> Vec<u8> {
    let mut buffer = initial;
    let mut position = 0usize;
    let mut payload = Vec::new();

    loop {
        // Chunk-size line.
        let line_end = loop {
            match find(&buffer[position..], b"\r\n") {
                Some(offset) => break position + offset,
                None if read_more(stream, &mut buffer).await => continue,
                None => return payload,
            }
        };

        let header = String::from_utf8_lossy(&buffer[position..line_end]).to_string();
        // Chunk extensions after ';' are not payload.
        let size =
            usize::from_str_radix(header.split(';').next().unwrap_or("").trim(), 16).unwrap_or(0);
        position = line_end + 2;

        if size == 0 {
            return payload;
        }

        while buffer.len() < position + size {
            if !read_more(stream, &mut buffer).await {
                payload.extend_from_slice(&buffer[position.min(buffer.len())..]);
                return payload;
            }
        }

        payload.extend_from_slice(&buffer[position..position + size]);
        // Skip the chunk's trailing CRLF.
        position += size + 2;
    }
}

async fn write_response(stream: &TcpStream, canned: &Canned) -> std::io::Result<()> {
    let response = format!(
        "HTTP/1.1 {} OK\r\nContent-Length: {}\r\nContent-Type: application/xml\r\nConnection: close\r\n\r\n{}",
        canned.status,
        canned.declared_length.unwrap_or(canned.body.len()),
        canned.body
    );
    let mut bytes = response.as_bytes();
    while !bytes.is_empty() {
        stream.writable().await?;
        match stream.try_write(bytes) {
            Ok(written) => bytes = &bytes[written..],
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn list_page(keys: &[&str], next_token: Option<&str>) -> String {
    let contents: String = keys
        .iter()
        .map(|key| {
            format!(
                "<Contents><Key>{key}</Key><Size>3</Size>\
                 <LastModified>2026-01-01T00:00:00.000Z</LastModified>\
                 <ETag>&quot;etag-{key}&quot;</ETag></Contents>"
            )
        })
        .collect();
    let truncated = next_token.is_some();
    let token = next_token
        .map(|token| format!("<NextContinuationToken>{token}</NextContinuationToken>"))
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>test-bucket</Name><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys>
<IsTruncated>{truncated}</IsTruncated>{token}{contents}</ListBucketResult>"#,
        keys.len()
    )
}

// --- the harness itself ---------------------------------------------------

/// The mock's chunked-body decoding is dead code today (every request the SDK
/// makes is Content-Length framed), so it is tested directly. A payload that
/// contains the chunk terminator byte sequence is the case a naive scan gets
/// wrong, silently dropping everything after it.
#[tokio::test]
async fn the_mock_decodes_chunked_bodies_including_terminator_lookalikes() {
    use tokio::io::AsyncWriteExt;

    let mock = MockR2::start(vec![Canned::ok("")]).await;
    let address = mock.endpoint.trim_start_matches("http://").to_string();

    let payload_a = "HELLO";
    // Payload bytes that look exactly like a chunk terminator.
    let payload_b = "\r\n0\r\n";
    let payload_c = "WORLD";

    let request = format!(
        "PUT /test-bucket/chunked.bin HTTP/1.1\r\nHost: {address}\r\n\
         Transfer-Encoding: chunked\r\n\r\n\
         {:X}\r\n{payload_a}\r\n{:X}\r\n{payload_b}\r\n{:X}\r\n{payload_c}\r\n0\r\n\r\n",
        payload_a.len(),
        payload_b.len(),
        payload_c.len(),
    );

    let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    // Let the server finish recording before the assertion reads it.
    let mut discard = Vec::new();
    let _ = tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut discard).await;

    let recorded = mock.first();
    let expected = format!("{payload_a}{payload_b}{payload_c}");
    assert_eq!(
        String::from_utf8_lossy(&recorded.body),
        expected,
        "chunk framing leaked into the recorded body, or a chunk was dropped"
    );
}

// --- wire format -----------------------------------------------------------

#[tokio::test]
async fn put_object_uses_path_style_addressing() {
    let mock = MockR2::start(vec![Canned::ok("")]).await;
    mock.client()
        .put_object("photos/cat.png", "bytes")
        .await
        .unwrap();

    let request = mock.first();
    assert_eq!(request.method, "PUT");
    assert!(
        request.target.starts_with("/test-bucket/photos/cat.png"),
        "expected path-style addressing, got {}",
        request.target
    );
    assert_eq!(request.body, b"bytes");
}

#[tokio::test]
async fn put_object_sends_the_resolved_content_type_and_metadata() {
    let mock = MockR2::start(vec![Canned::ok("")]).await;
    mock.client()
        .put_object_with(
            "reports/q3.pdf",
            "data",
            PutOptions::new()
                .cache_control("public, max-age=60")
                .metadata("origin", "test"),
        )
        .await
        .unwrap();

    let request = mock.first();
    // Guessed from the extension, since PutOptions did not set one.
    assert_eq!(request.header("content-type"), Some("application/pdf"));
    assert_eq!(request.header("cache-control"), Some("public, max-age=60"));
    assert_eq!(request.header("x-amz-meta-origin"), Some("test"));
}

/// The regression test for R2's aws-chunked handling: a *streamed* body must
/// not be framed as `aws-chunked`, because R2 stores that framing header on the
/// object instead of stripping it.
#[tokio::test]
async fn streamed_uploads_are_not_framed_as_aws_chunked() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("payload.bin");
    tokio::fs::write(&path, vec![b'x'; 64 * 1024])
        .await
        .unwrap();

    let mock = MockR2::start(vec![Canned::ok("")]).await;
    mock.client()
        .upload_file("streamed.bin", &path)
        .await
        .unwrap();

    let request = mock.first();
    let encodings = request.header_values("content-encoding");
    assert!(
        !encodings.iter().any(|value| value.contains("aws-chunked")),
        "streamed upload was framed as aws-chunked: {encodings:?}"
    );
    assert_eq!(request.header("x-amz-trailer"), None);
    assert_eq!(request.header("x-amz-decoded-content-length"), None);
    assert_eq!(request.header("content-length"), Some("65536"));
}

#[tokio::test]
async fn a_caller_supplied_content_encoding_survives_intact() {
    let mock = MockR2::start(vec![Canned::ok("")]).await;
    mock.client()
        .put_object_with(
            "compressed.json",
            "payload",
            PutOptions::new().content_encoding("gzip"),
        )
        .await
        .unwrap();

    assert_eq!(mock.first().header_values("content-encoding"), vec!["gzip"]);
}

#[tokio::test]
async fn delete_objects_still_sends_its_required_checksum() {
    // Turning checksums down to "when required" must not break the operations
    // that genuinely require one.
    let mock = MockR2::start(vec![Canned::ok(
        r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult><Deleted><Key>a.txt</Key></Deleted></DeleteResult>"#,
    )])
    .await;

    mock.client().delete_objects(vec!["a.txt"]).await.unwrap();

    let request = mock.first();
    let has_checksum = request
        .headers
        .iter()
        .any(|(name, _)| name.starts_with("x-amz-checksum-") || name == "content-md5");
    assert!(
        has_checksum,
        "DeleteObjects must carry a checksum; headers were {:?}",
        request.headers
    );
}

#[tokio::test]
async fn range_requests_send_an_inclusive_range_header() {
    let mock = MockR2::start(vec![Canned::ok("hello")]).await;
    mock.client()
        .get_object_range("a.txt", 0, Some(4))
        .await
        .unwrap();
    assert_eq!(mock.first().header("range"), Some("bytes=0-4"));
}

#[tokio::test]
async fn copy_object_percent_encodes_the_source_key() {
    let mock = MockR2::start(vec![Canned::ok(
        r#"<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>&quot;e&quot;</ETag></CopyObjectResult>"#,
    )])
    .await;

    mock.client()
        .copy_object("my folder/a+b.png", "dest.png")
        .await
        .unwrap();

    assert_eq!(
        mock.first().header("x-amz-copy-source"),
        Some("test-bucket/my%20folder/a%2Bb.png")
    );
}

// --- pagination and batching ----------------------------------------------

#[tokio::test]
async fn list_all_objects_follows_continuation_tokens() {
    let mock = MockR2::start(vec![
        Canned::ok(list_page(&["a.txt", "b.txt"], Some("TOKEN-2"))),
        Canned::ok(list_page(&["c.txt"], None)),
    ])
    .await;

    let objects = mock.client().list_all_objects(None).await.unwrap();
    let keys: Vec<&str> = objects.iter().map(|object| object.key.as_str()).collect();
    assert_eq!(keys, ["a.txt", "b.txt", "c.txt"]);

    let requests = mock.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].target.contains("continuation-token=TOKEN-2"),
        "second page did not carry the token: {}",
        requests[1].target
    );
}

#[tokio::test]
async fn a_truncated_page_without_a_token_terminates_instead_of_looping() {
    // A server that claims truncation but returns no token would spin forever
    // if the loop trusted `is_truncated` alone.
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>test-bucket</Name><IsTruncated>true</IsTruncated>
<Contents><Key>a.txt</Key><Size>1</Size></Contents></ListBucketResult>"#;

    let mock = MockR2::start(vec![Canned::ok(body)]).await;
    let objects = mock.client().list_all_objects(None).await.unwrap();

    assert_eq!(objects.len(), 1);
    assert_eq!(mock.requests().len(), 1);
}

#[tokio::test]
async fn delete_objects_batches_at_a_thousand_keys() {
    let keys: Vec<String> = (0..1001).map(|index| format!("key-{index}")).collect();
    let mock = MockR2::start(vec![
        Canned::ok(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>"#),
        Canned::ok(r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult></DeleteResult>"#),
    ])
    .await;

    mock.client().delete_objects(keys).await.unwrap();

    let requests = mock.requests();
    assert_eq!(
        requests.len(),
        2,
        "1001 keys should split into two requests"
    );
    let first = String::from_utf8_lossy(&requests[0].body);
    let second = String::from_utf8_lossy(&requests[1].body);
    assert_eq!(first.matches("<Key>").count(), 1000);
    assert_eq!(second.matches("<Key>").count(), 1);
}

#[tokio::test]
async fn delete_objects_reports_per_key_failures() {
    let mock = MockR2::start(vec![Canned::ok(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult><Deleted><Key>a.txt</Key></Deleted>
<Error><Key>b.txt</Key><Code>AccessDenied</Code><Message>nope</Message></Error></DeleteResult>"#,
    )])
    .await;

    let report = mock
        .client()
        .delete_objects(vec!["a.txt", "b.txt"])
        .await
        .unwrap();

    assert!(!report.all_succeeded());
    assert_eq!(report.deleted, ["a.txt"]);
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].key, "b.txt");
    assert_eq!(report.failed[0].code.as_deref(), Some("AccessDenied"));
}

#[tokio::test]
async fn an_invalid_key_aborts_the_batch_before_anything_is_deleted() {
    let mock = MockR2::start(vec![Canned::ok("")]).await;

    // The bad key is in the second batch; nothing at all should be sent.
    let mut keys: Vec<String> = (0..1000).map(|index| format!("key-{index}")).collect();
    keys.push(String::new());

    let err = mock.client().delete_objects(keys).await.unwrap_err();
    assert!(matches!(err, Error::InvalidArgument { .. }));
    assert!(
        mock.requests().is_empty(),
        "keys were deleted before validation failed"
    );
}

#[tokio::test]
async fn list_prefixes_returns_common_prefixes() {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>test-bucket</Name><IsTruncated>false</IsTruncated>
<CommonPrefixes><Prefix>photos/2024/</Prefix></CommonPrefixes>
<CommonPrefixes><Prefix>photos/2025/</Prefix></CommonPrefixes></ListBucketResult>"#;

    let mock = MockR2::start(vec![Canned::ok(body)]).await;
    let prefixes = mock.client().list_prefixes("photos/").await.unwrap();

    assert_eq!(prefixes, ["photos/2024/", "photos/2025/"]);
    assert!(mock.first().target.contains("delimiter=%2F"));
}

// --- error mapping ---------------------------------------------------------

#[tokio::test]
async fn a_missing_object_becomes_object_not_found() {
    let mock = MockR2::start(vec![Canned::error(404, "NoSuchKey", "not here")]).await;
    let err = mock.client().get_object("gone.txt").await.unwrap_err();

    assert!(err.is_not_found());
    assert_eq!(err.status(), Some(404));
    match err {
        Error::ObjectNotFound { bucket, key } => {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(key, "gone.txt");
        }
        other => panic!("expected ObjectNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn object_exists_is_false_for_404_and_an_error_for_403() {
    let mock = MockR2::start(vec![Canned::error(404, "NoSuchKey", "not here")]).await;
    assert!(!mock.client().object_exists("gone.txt").await.unwrap());

    let forbidden = MockR2::start(vec![Canned::error(403, "AccessDenied", "denied")]).await;
    let err = forbidden
        .client()
        .object_exists("secret.txt")
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(403));
    assert!(!err.is_not_found());
}

#[tokio::test]
async fn api_errors_carry_the_service_message_not_just_service_error() {
    let mock = MockR2::start(vec![Canned::error(
        403,
        "AccessDenied",
        "Access Denied by policy",
    )])
    .await;

    let err = mock.client().put_object("a.txt", "x").await.unwrap_err();
    let rendered = err.to_string();
    assert!(
        rendered.contains("Access Denied by policy"),
        "message was flattened away: {rendered}"
    );
    assert!(std::error::Error::source(&err).is_some());
}

#[tokio::test]
async fn missing_cors_configuration_reads_as_no_rules() {
    let mock = MockR2::start(vec![Canned::error(
        404,
        "NoSuchCORSConfiguration",
        "The CORS configuration does not exist",
    )])
    .await;

    assert!(mock.client().get_cors().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_missing_bucket_is_not_reported_as_no_cors_rules() {
    // The distinction that a blanket 404-to-empty mapping would destroy.
    let mock = MockR2::start(vec![Canned::error(404, "NoSuchBucket", "no bucket")]).await;
    let err = mock.client().get_cors().await.unwrap_err();

    assert!(matches!(err, Error::BucketNotFound { .. }), "{err:?}");
}

#[tokio::test]
async fn a_missing_lifecycle_configuration_reads_as_no_rules() {
    let mock = MockR2::start(vec![Canned::error(
        404,
        "NoSuchLifecycleConfiguration",
        "does not exist",
    )])
    .await;

    assert!(mock.client().get_lifecycle().await.unwrap().is_empty());
}

// --- upload paths ----------------------------------------------------------

#[tokio::test]
async fn upload_file_honours_a_custom_multipart_threshold() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("small.bin");
    tokio::fs::write(&path, vec![b'x'; 32 * 1024])
        .await
        .unwrap();

    // Threshold above the file size: one plain PUT.
    let single = MockR2::start(vec![Canned::ok("")]).await;
    single
        .client()
        .upload_file_with(
            "small.bin",
            &path,
            MultipartOptions::new().threshold(64 * 1024),
        )
        .await
        .unwrap();
    assert_eq!(single.requests().len(), 1);
    assert!(!single.first().target.contains("uploads"));

    // Threshold below it: the multipart handshake starts instead.
    let multipart = MockR2::start(vec![Canned::ok(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket><Key>small.bin</Key>
<UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"#,
    )])
    .await;
    let _ = multipart
        .client()
        .upload_file_with(
            "small.bin",
            &path,
            MultipartOptions::new()
                .threshold(1024)
                .part_size(5 * 1024 * 1024),
        )
        .await;
    assert!(
        multipart.first().target.contains("uploads"),
        "expected CreateMultipartUpload, got {}",
        multipart.first().target
    );
}

#[tokio::test]
async fn a_failed_part_aborts_the_upload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("big.bin");
    tokio::fs::write(&path, vec![b'x'; 6 * 1024 * 1024])
        .await
        .unwrap();

    let mock = MockR2::start(vec![
        Canned::ok(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>"#,
        ),
        Canned::error(500, "InternalError", "part failed"),
        Canned::ok(""),
    ])
    .await;

    let err = mock
        .client()
        .multipart_upload_file(
            "big.bin",
            &path,
            MultipartOptions::new()
                .part_size(5 * 1024 * 1024)
                .concurrency(1),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status(), Some(500));

    let requests = mock.requests();
    let aborted = requests
        .iter()
        .any(|request| request.method == "DELETE" && request.target.contains("uploadId=upload-1"));
    assert!(aborted, "no abort was sent: {requests:#?}");
}

// --- downloads -------------------------------------------------------------

#[tokio::test]
async fn download_to_writes_the_body_and_reports_its_length() {
    let mock = MockR2::start(vec![Canned::ok("downloaded bytes")]).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("nested/out.txt");

    let written = mock
        .client()
        .download_to("a.txt", &destination)
        .await
        .unwrap();

    assert_eq!(written, 16);
    assert_eq!(
        tokio::fs::read_to_string(&destination).await.unwrap(),
        "downloaded bytes"
    );
}

#[tokio::test]
async fn a_download_that_404s_creates_no_file() {
    let mock = MockR2::start(vec![Canned::error(404, "NoSuchKey", "gone")]).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("out.txt");

    let err = mock
        .client()
        .download_to("gone.txt", &destination)
        .await
        .unwrap_err();
    assert!(err.is_not_found());
    assert!(!destination.exists());
}

/// The real test of the atomic-download fix: the failure must happen *after*
/// bytes have been written, which a 404 never reaches. The server promises 4096
/// bytes, sends 10, and hangs up.
#[tokio::test]
async fn a_download_that_fails_mid_stream_leaves_nothing_behind() {
    let mock = MockR2::start(vec![Canned::truncated("0123456789", 4096)]).await;
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("out.txt");

    let err = mock
        .client()
        .download_to("partial.bin", &destination)
        .await
        .unwrap_err();
    assert!(
        !err.is_not_found(),
        "expected a transfer failure, got {err}"
    );

    assert!(
        !destination.exists(),
        "a truncated file was left at the destination"
    );

    // No temporary file orphaned beside it either.
    let mut entries = tokio::fs::read_dir(directory.path()).await.unwrap();
    let leftover = entries.next_entry().await.unwrap();
    assert!(
        leftover.is_none(),
        "orphaned file left behind: {:?}",
        leftover.map(|entry| entry.file_name())
    );
}

#[tokio::test]
async fn download_file_mirrors_the_key_under_the_directory() {
    let mock = MockR2::start(vec![Canned::ok("body")]).await;
    let directory = tempfile::tempdir().unwrap();

    let written = mock
        .client()
        .download_file("photos/2024/cat.png", directory.path())
        .await
        .unwrap();

    assert_eq!(written, directory.path().join("photos/2024/cat.png"));
    assert!(written.exists());
}

#[tokio::test]
async fn download_to_refuses_a_directory_destination_before_transferring() {
    let mock = MockR2::start(vec![Canned::ok("body")]).await;
    let directory = tempfile::tempdir().unwrap();

    let err = mock
        .client()
        .download_to("a.txt", directory.path())
        .await
        .unwrap_err();

    assert!(matches!(err, Error::InvalidArgument { .. }), "{err:?}");
    assert!(
        mock.requests().is_empty(),
        "the body was transferred before the destination was checked"
    );
}

#[tokio::test]
async fn a_long_key_can_still_be_downloaded() {
    // 250 bytes: a legal key and a legal filename. The temporary file used for
    // the atomic write must not push it past the 255-byte name limit.
    let long_name = "n".repeat(250);
    let mock = MockR2::start(vec![Canned::ok("body")]).await;
    let directory = tempfile::tempdir().unwrap();

    let written = mock
        .client()
        .download_file(&long_name, directory.path())
        .await
        .unwrap();

    assert_eq!(tokio::fs::read_to_string(&written).await.unwrap(), "body");
}

#[tokio::test]
async fn a_missing_copy_source_names_the_source_bucket() {
    let mock = MockR2::start(vec![Canned::error(404, "NoSuchKey", "gone")]).await;

    let err = mock
        .client()
        .copy_object_from("other-bucket", "src.txt", "dest.txt")
        .await
        .unwrap_err();

    match err {
        Error::ObjectNotFound { bucket, key } => {
            assert_eq!(bucket, "other-bucket", "named the wrong bucket");
            assert_eq!(key, "src.txt");
        }
        other => panic!("expected ObjectNotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn an_unparseable_error_body_still_yields_an_actionable_message() {
    // A 5xx whose body is not S3 XML: the SDK chain bottoms out in a bare
    // "Error", so the status is what makes the failure actionable.
    let mock = MockR2::start(vec![Canned {
        status: 502,
        body: "<html>502 Bad Gateway</html>".to_string(),
        declared_length: None,
    }])
    .await;

    let err = mock.client().put_object("a.txt", "x").await.unwrap_err();
    let rendered = err.to_string();

    assert_eq!(err.status(), Some(502));
    assert!(rendered.contains("502"), "no status in message: {rendered}");
    assert!(
        rendered.contains("put_object"),
        "no operation in message: {rendered}"
    );
}

#[tokio::test]
async fn a_completed_part_can_be_rebuilt_and_completed() {
    // The resume-across-processes path: part numbers and ETags come back from
    // storage rather than from upload_part.
    let mock = MockR2::start(vec![Canned::ok(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult><ETag>&quot;final&quot;</ETag></CompleteMultipartUploadResult>"#,
    )])
    .await;

    let parts = vec![
        cloudflare_r2_rs::CompletedPart::new(1, "\"etag-1\""),
        cloudflare_r2_rs::CompletedPart::new(2, "\"etag-2\""),
    ];

    let outcome = mock
        .client()
        .complete_multipart_upload("big.bin", "upload-1", parts)
        .await
        .unwrap();
    assert_eq!(outcome.key, "big.bin");

    let request = mock.first();
    let body = String::from_utf8_lossy(&request.body);
    assert!(body.contains("<PartNumber>1</PartNumber>"), "{body}");
    assert!(body.contains("<PartNumber>2</PartNumber>"), "{body}");
}

#[tokio::test]
async fn download_file_refuses_a_key_that_would_escape_the_directory() {
    let mock = MockR2::start(vec![Canned::ok("body")]).await;
    let directory = tempfile::tempdir().unwrap();

    for key in ["/etc/passwd", "../escape.txt", "a/../../escape.txt"] {
        let err = mock
            .client()
            .download_file(key, directory.path())
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidArgument { .. }),
            "key {key:?} was not refused"
        );
    }

    assert!(
        mock.requests().is_empty(),
        "an escaping key still reached the network"
    );
}
