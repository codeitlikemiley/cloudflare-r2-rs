//! Object-level operations: put, get, head, copy, delete, list and download.

use std::path::{Component, Path, PathBuf};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use tokio::io::AsyncWriteExt;

use crate::client::R2Client;
use crate::error::{from_sdk, Error, Result};
use crate::types::{
    DeleteFailure, DeleteReport, ListOptions, ListPage, ObjectMetadata, ObjectSummary, PutOptions,
    PutOutcome, MAX_DELETE_BATCH,
};

impl R2Client {
    /// Stores an object, guessing its content type from the key's extension.
    ///
    /// The body can be anything convertible into a [`ByteStream`], which
    /// includes `Vec<u8>` and `bytes::Bytes`. To stream from disk without
    /// buffering the whole file, use [`upload_file`](R2Client::upload_file).
    pub async fn put_object(&self, key: &str, body: impl Into<ByteStream>) -> Result<PutOutcome> {
        self.put_object_with(key, body, PutOptions::new()).await
    }

    /// Stores an object with explicit headers and user metadata.
    pub async fn put_object_with(
        &self,
        key: &str,
        body: impl Into<ByteStream>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        validate_key(key)?;

        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .content_type(options.resolved_content_type(key))
            .set_cache_control(options.cache_control.clone())
            .set_content_disposition(options.content_disposition.clone())
            .set_content_encoding(options.content_encoding.clone())
            .set_content_language(options.content_language.clone());

        for (name, value) in &options.metadata {
            request = request.metadata(name, value);
        }

        let response = request
            .send()
            .await
            .map_err(|err| from_sdk("put_object", err))?;

        Ok(PutOutcome {
            key: key.to_string(),
            etag: response.e_tag,
        })
    }

    /// Fetches an object's full body into memory.
    ///
    /// Fails with [`Error::ObjectNotFound`] when the key does not exist.
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let stream = self.get_object_stream(key).await?;
        let bytes = stream
            .collect()
            .await
            .map_err(|err| Error::Body(Box::new(err)))?;
        Ok(bytes.into_bytes().to_vec())
    }

    /// Fetches a byte range of an object, `start` and `end` both inclusive.
    ///
    /// Passing `None` for `end` reads to the end of the object.
    pub async fn get_object_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<u8>> {
        validate_key(key)?;

        if let Some(end) = end {
            if end < start {
                return Err(Error::invalid_argument(
                    "end",
                    format!("range end {end} is before range start {start}"),
                ));
            }
        }

        let range = match end {
            Some(end) => format!("bytes={start}-{end}"),
            None => format!("bytes={start}-"),
        };

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|err| self.map_object_error("get_object", key, err))?;

        let bytes = response
            .body
            .collect()
            .await
            .map_err(|err| Error::Body(Box::new(err)))?;
        Ok(bytes.into_bytes().to_vec())
    }

    /// Opens an object's body as a stream, without buffering it in memory.
    pub async fn get_object_stream(&self, key: &str) -> Result<ByteStream> {
        validate_key(key)?;

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| self.map_object_error("get_object", key, err))?;

        Ok(response.body)
    }

    /// Reads an object's metadata without transferring its body.
    pub async fn head_object(&self, key: &str) -> Result<ObjectMetadata> {
        validate_key(key)?;

        let response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| self.map_object_error("head_object", key, err))?;

        Ok(ObjectMetadata {
            content_length: response.content_length.unwrap_or_default(),
            content_type: response.content_type,
            etag: response.e_tag,
            last_modified: response.last_modified,
            cache_control: response.cache_control,
            content_disposition: response.content_disposition,
            content_encoding: response.content_encoding,
            content_language: response.content_language,
            metadata: response.metadata.unwrap_or_default(),
        })
    }

    /// Reports whether an object exists.
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        match self.head_object(key).await {
            Ok(_) => Ok(true),
            Err(err) if err.is_not_found() => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Copies an object within this bucket.
    pub async fn copy_object(&self, source_key: &str, destination_key: &str) -> Result<()> {
        self.copy_object_from(&self.bucket.clone(), source_key, destination_key)
            .await
    }

    /// Copies an object from another bucket on the same account into this one.
    pub async fn copy_object_from(
        &self,
        source_bucket: &str,
        source_key: &str,
        destination_key: &str,
    ) -> Result<()> {
        validate_key(source_key)?;
        validate_key(destination_key)?;

        if source_bucket.trim().is_empty() {
            return Err(Error::invalid_argument(
                "source_bucket",
                "source bucket must not be empty",
            ));
        }

        let copy_source = format!("{source_bucket}/{}", encode_copy_source(source_key));

        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(destination_key)
            .copy_source(copy_source)
            .send()
            .await
            .map_err(|err| self.map_object_error("copy_object", source_key, err))?;
        Ok(())
    }

    /// Deletes a single object.
    ///
    /// Deleting a key that does not exist succeeds, matching S3 semantics.
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        validate_key(key)?;

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| from_sdk("delete_object", err))?;
        Ok(())
    }

    /// Deletes many objects, batching into requests of
    /// [`MAX_DELETE_BATCH`] keys.
    ///
    /// Per-key failures are reported in the returned [`DeleteReport`] rather
    /// than aborting the whole operation; check
    /// [`all_succeeded`](DeleteReport::all_succeeded).
    pub async fn delete_objects<I, K>(&self, keys: I) -> Result<DeleteReport>
    where
        I: IntoIterator<Item = K>,
        K: Into<String>,
    {
        let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
        let mut report = DeleteReport::default();

        for batch in keys.chunks(MAX_DELETE_BATCH) {
            let mut identifiers = Vec::with_capacity(batch.len());
            for key in batch {
                validate_key(key)?;
                identifiers.push(
                    ObjectIdentifier::builder()
                        .key(key)
                        .build()
                        .map_err(|err| Error::invalid_argument("keys", err.to_string()))?,
                );
            }

            let delete = Delete::builder()
                .set_objects(Some(identifiers))
                .build()
                .map_err(|err| Error::invalid_argument("keys", err.to_string()))?;

            let response = self
                .client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await
                .map_err(|err| from_sdk("delete_objects", err))?;

            report.deleted.extend(
                response
                    .deleted
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|deleted| deleted.key),
            );
            report.failed.extend(
                response
                    .errors
                    .unwrap_or_default()
                    .into_iter()
                    .map(|error| DeleteFailure {
                        key: error.key.unwrap_or_default(),
                        code: error.code,
                        message: error.message,
                    }),
            );
        }

        Ok(report)
    }

    /// Deletes every object under a prefix.
    ///
    /// Refuses an empty prefix — use [`delete_objects`](R2Client::delete_objects)
    /// with an explicit list if you really mean to empty the bucket.
    pub async fn delete_prefix(&self, prefix: &str) -> Result<DeleteReport> {
        if prefix.is_empty() {
            return Err(Error::invalid_argument(
                "prefix",
                "refusing to delete with an empty prefix, which would empty the bucket",
            ));
        }

        let keys: Vec<String> = self
            .list_all_objects(Some(prefix))
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect();

        if keys.is_empty() {
            return Ok(DeleteReport::default());
        }

        self.delete_objects(keys).await
    }

    /// Lists one page of objects.
    ///
    /// Follow [`ListPage::next_continuation_token`] to page through the rest,
    /// or use [`list_all_objects`](R2Client::list_all_objects).
    pub async fn list_objects(&self, options: ListOptions) -> Result<ListPage> {
        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .set_prefix(options.prefix)
            .set_delimiter(options.delimiter)
            .set_max_keys(options.max_keys)
            .set_start_after(options.start_after)
            .set_continuation_token(options.continuation_token)
            .send()
            .await
            .map_err(|err| from_sdk("list_objects_v2", err))?;

        let objects = response
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|object| {
                object.key.map(|key| ObjectSummary {
                    key,
                    size: object.size.unwrap_or_default(),
                    etag: object.e_tag,
                    last_modified: object.last_modified,
                })
            })
            .collect();

        let common_prefixes = response
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .filter_map(|prefix| prefix.prefix)
            .collect();

        Ok(ListPage {
            objects,
            common_prefixes,
            next_continuation_token: response.next_continuation_token,
            is_truncated: response.is_truncated.unwrap_or(false),
        })
    }

    /// Lists every object under an optional prefix, following pagination.
    pub async fn list_all_objects(&self, prefix: Option<&str>) -> Result<Vec<ObjectSummary>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut options = ListOptions::new();
            if let Some(prefix) = prefix {
                options = options.prefix(prefix);
            }
            options.continuation_token = continuation_token;

            let page = self.list_objects(options).await?;
            objects.extend(page.objects);

            match page.next_continuation_token {
                // Guard against a truncated page with no token, which would
                // otherwise loop forever re-reading page one.
                Some(token) if page.is_truncated => continuation_token = Some(token),
                _ => break,
            }
        }

        Ok(objects)
    }

    /// Lists every key in the bucket.
    pub async fn list_keys(&self) -> Result<Vec<String>> {
        Ok(self
            .list_all_objects(None)
            .await?
            .into_iter()
            .map(|object| object.key)
            .collect())
    }

    /// Lists the immediate "folders" under a prefix, using `/` as the delimiter.
    pub async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut prefixes = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut options = ListOptions::new().prefix(prefix).delimiter("/");
            options.continuation_token = continuation_token;

            let page = self.list_objects(options).await?;
            prefixes.extend(page.common_prefixes);

            match page.next_continuation_token {
                Some(token) if page.is_truncated => continuation_token = Some(token),
                _ => break,
            }
        }

        Ok(prefixes)
    }

    /// Downloads an object into `directory`, mirroring the key's path.
    ///
    /// A key of `photos/2024/cat.png` lands at `<directory>/photos/2024/cat.png`,
    /// creating intermediate directories as needed. Returns the written path.
    pub async fn download_file(&self, key: &str, directory: &Path) -> Result<PathBuf> {
        validate_key(key)?;

        if !directory.is_dir() {
            return Err(Error::invalid_argument(
                "directory",
                format!("{} is not a directory", directory.display()),
            ));
        }

        let destination = directory.join(key);
        self.download_to(key, &destination).await?;
        Ok(destination)
    }

    /// Downloads an object to an exact path, streaming it to disk.
    ///
    /// Creates parent directories as needed and returns the number of bytes
    /// written.
    pub async fn download_to(&self, key: &str, destination: &Path) -> Result<u64> {
        validate_key(key)?;

        if let Some(parent) = destination.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        let mut stream = self.get_object_stream(key).await?;
        let mut file = tokio::fs::File::create(destination).await?;
        let mut written = 0u64;

        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|err| Error::Body(Box::new(err)))?
        {
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }

        file.flush().await?;
        Ok(written)
    }

    /// Turns a 404 on a keyed operation into [`Error::ObjectNotFound`], so
    /// callers can match on "missing" without inspecting status codes.
    pub(crate) fn map_object_error<E>(
        &self,
        operation: &'static str,
        key: &str,
        err: aws_sdk_s3::error::SdkError<E, aws_sdk_s3::config::http::HttpResponse>,
    ) -> Error
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let err = from_sdk(operation, err);
        if err.is_not_found() {
            Error::ObjectNotFound {
                bucket: self.bucket.clone(),
                key: key.to_string(),
            }
        } else {
            err
        }
    }
}

/// Rejects keys that are empty or that would escape a download directory.
pub(crate) fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::invalid_argument("key", "key must not be empty"));
    }

    if Path::new(key)
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Error::invalid_argument(
            "key",
            format!("key `{key}` contains a `..` segment"),
        ));
    }

    Ok(())
}

/// Percent-encodes a key for use in a `x-amz-copy-source` header.
///
/// `/` is left intact because the header is `bucket/key` and R2 expects the
/// key's own slashes to stay literal.
pub(crate) fn encode_copy_source(key: &str) -> String {
    let mut encoded = String::with_capacity(key.len());
    for byte in key.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                encoded.push(*byte as char)
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_key() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn rejects_traversal_key() {
        assert!(validate_key("../../etc/passwd").is_err());
        assert!(validate_key("a/../../b").is_err());
    }

    #[test]
    fn accepts_normal_keys() {
        assert!(validate_key("photos/2024/cat.png").is_ok());
        assert!(validate_key("file.txt").is_ok());
    }

    #[test]
    fn copy_source_leaves_safe_characters_alone() {
        assert_eq!(encode_copy_source("photos/cat-1.png"), "photos/cat-1.png");
    }

    #[test]
    fn copy_source_encodes_spaces_and_specials() {
        assert_eq!(
            encode_copy_source("my folder/a+b&c.png"),
            "my%20folder/a%2Bb%26c.png"
        );
    }

    #[test]
    fn copy_source_encodes_non_ascii() {
        assert_eq!(encode_copy_source("caf\u{e9}.txt"), "caf%C3%A9.txt");
    }
}
