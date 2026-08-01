//! Conversion of everyday Rust byte containers into request bodies.

use aws_sdk_s3::primitives::{ByteStream, SdkBody};

/// Anything that can be uploaded as an object body.
///
/// `ByteStream` alone only converts from `Vec<u8>`, `Bytes` and `SdkBody`,
/// which makes the obvious calls — `put_object("k", "hello")`,
/// `put_object("k", b"hello")`, `put_object("k", &buffer)` — fail to compile.
/// This trait covers the ordinary cases as well, so reaching for a body type
/// is never a step in using the crate.
///
/// Borrowed inputs are copied into an owned buffer, since the request body
/// must outlive the call. Pass a [`ByteStream`] to stream instead of buffering,
/// or use [`upload_file`](crate::R2Client::upload_file) to stream from disk.
///
/// ```
/// use cloudflare_r2_rs::IntoBody;
///
/// // All of these are valid bodies.
/// let _ = "hello".into_body();
/// let _ = String::from("hello").into_body();
/// let _ = b"hello".into_body();
/// let _ = vec![1u8, 2, 3].into_body();
/// let _ = [1u8, 2, 3].as_slice().into_body();
/// let _ = (&String::from("hello")).into_body();
/// ```
pub trait IntoBody {
    /// Converts `self` into a request body.
    fn into_body(self) -> ByteStream;
}

impl IntoBody for ByteStream {
    fn into_body(self) -> ByteStream {
        self
    }
}

impl IntoBody for Vec<u8> {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self)
    }
}

impl IntoBody for bytes::Bytes {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self)
    }
}

impl IntoBody for &[u8] {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.to_vec())
    }
}

impl IntoBody for &Vec<u8> {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.clone())
    }
}

impl IntoBody for &String {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.as_bytes().to_vec())
    }
}

impl IntoBody for SdkBody {
    fn into_body(self) -> ByteStream {
        ByteStream::new(self)
    }
}

impl<const N: usize> IntoBody for &[u8; N] {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.to_vec())
    }
}

impl<const N: usize> IntoBody for [u8; N] {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.to_vec())
    }
}

impl IntoBody for String {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.into_bytes())
    }
}

impl IntoBody for &str {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.as_bytes().to_vec())
    }
}

impl IntoBody for std::borrow::Cow<'_, [u8]> {
    fn into_body(self) -> ByteStream {
        ByteStream::from(self.into_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn collect(body: impl IntoBody) -> Vec<u8> {
        body.into_body()
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec()
    }

    #[tokio::test]
    async fn every_common_container_round_trips() {
        assert_eq!(collect("hi").await, b"hi");
        assert_eq!(collect(String::from("hi")).await, b"hi");
        assert_eq!(collect(b"hi").await, b"hi");
        assert_eq!(collect(*b"hi").await, b"hi");
        assert_eq!(collect(vec![b'h', b'i']).await, b"hi");
        assert_eq!(collect(&vec![b'h', b'i']).await, b"hi");
        assert_eq!(collect([b'h', b'i'].as_slice()).await, b"hi");
        assert_eq!(collect(bytes::Bytes::from_static(b"hi")).await, b"hi");
        assert_eq!(collect(ByteStream::from_static(b"hi")).await, b"hi");
        assert_eq!(collect(&String::from("hi")).await, b"hi");
        // SdkBody kept working when the signature moved off `Into<ByteStream>`.
        assert_eq!(collect(SdkBody::from("hi")).await, b"hi");
    }

    #[tokio::test]
    async fn empty_bodies_are_allowed() {
        assert!(collect("").await.is_empty());
        assert!(collect(Vec::new()).await.is_empty());
    }
}
