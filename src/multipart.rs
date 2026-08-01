//! Multipart uploads, both automatic and hand-driven.

use std::path::Path;

use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart as SdkCompletedPart};
use futures::stream::StreamExt;

use crate::body::IntoBody;
use crate::client::R2Client;
use crate::error::{from_sdk, Error, Result};
use crate::object::validate_key;
use crate::types::{
    CompletedPart, MultipartOptions, MultipartUpload, PutOptions, PutOutcome, MAX_PARTS,
    MIN_PART_SIZE,
};

impl R2Client {
    /// Uploads a local file, choosing single-shot or multipart automatically.
    ///
    /// Files below [`DEFAULT_MULTIPART_THRESHOLD`](crate::DEFAULT_MULTIPART_THRESHOLD)
    /// are sent in one request; larger ones are split into concurrent parts.
    /// Either way the body is streamed from disk rather than buffered in
    /// memory.
    pub async fn upload_file(&self, key: &str, path: &Path) -> Result<PutOutcome> {
        self.upload_file_with(key, path, MultipartOptions::new())
            .await
    }

    /// Uploads a local file with explicit part size, concurrency and metadata.
    pub async fn upload_file_with(
        &self,
        key: &str,
        path: &Path,
        options: MultipartOptions,
    ) -> Result<PutOutcome> {
        validate_key(key)?;

        let size = file_size(path).await?;
        if size < options.threshold {
            let body = ByteStream::from_path(path).await.map_err(|err| {
                Error::file(path, "could not open the file for upload", Some(err))
            })?;
            return self.put_object_with(key, body, options.put_options).await;
        }

        self.multipart_upload_file(key, path, options).await
    }

    /// Uploads a local file using multipart regardless of its size.
    ///
    /// Every part is the same size except the last, which is what R2 requires
    /// of a multipart upload.
    ///
    /// If a part fails, the upload is aborted on a best-effort basis before the
    /// error is returned. Should that abort itself fail — or should the process
    /// die mid-upload — the incomplete upload keeps holding storage until it is
    /// cleaned up; [`list_multipart_uploads`](R2Client::list_multipart_uploads)
    /// finds those, and a lifecycle rule can expire them automatically.
    pub async fn multipart_upload_file(
        &self,
        key: &str,
        path: &Path,
        options: MultipartOptions,
    ) -> Result<PutOutcome> {
        validate_key(key)?;

        if options.concurrency == 0 {
            return Err(Error::invalid_argument(
                "concurrency",
                "concurrency must be at least 1",
            ));
        }

        let size = file_size(path).await?;
        if size == 0 {
            return Err(Error::multipart(
                key,
                "cannot multipart-upload an empty file; use put_object instead",
            ));
        }

        let part_size = effective_part_size(size, options.part_size)?;
        let part_count = size.div_ceil(part_size);

        let upload = self
            .create_multipart_upload(key, options.put_options)
            .await?;

        let result = self
            .upload_all_parts(
                key,
                &upload.upload_id,
                path,
                size,
                part_size,
                part_count,
                options.concurrency,
            )
            .await;

        let parts = match result {
            Ok(parts) => parts,
            Err(err) => {
                // Best effort: the original failure is what the caller needs to
                // see, so a failed abort must not mask it.
                if let Err(abort_err) = self.abort_multipart_upload(key, &upload.upload_id).await {
                    log::warn!(
                        "failed to abort multipart upload {} for {key}: {abort_err}",
                        upload.upload_id
                    );
                }
                return Err(err);
            }
        };

        self.complete_multipart_upload(key, &upload.upload_id, parts)
            .await
    }

    /// Uploads every part, at most `concurrency` in flight, and returns them
    /// ordered by part number.
    #[allow(clippy::too_many_arguments)]
    async fn upload_all_parts(
        &self,
        key: &str,
        upload_id: &str,
        path: &Path,
        size: u64,
        part_size: u64,
        part_count: u64,
        concurrency: usize,
    ) -> Result<Vec<CompletedPart>> {
        let mut parts: Vec<CompletedPart> = futures::stream::iter(0..part_count)
            .map(|index| {
                let offset = index * part_size;
                // The final part takes whatever is left, which is never zero
                // because part_count is a ceiling division of a non-zero size.
                let length = part_size.min(size - offset);
                let part_number = (index + 1) as i32;

                async move {
                    let body = ByteStream::read_from()
                        .path(path)
                        .offset(offset)
                        .length(Length::Exact(length))
                        .build()
                        .await
                        .map_err(|err| {
                            Error::file(path, "could not read the part from disk", Some(err))
                        })?;

                    self.upload_part(key, upload_id, part_number, body).await
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        parts.sort_by_key(|part| part.part_number);
        Ok(parts)
    }

    /// Starts a multipart upload and returns its handle.
    pub async fn create_multipart_upload(
        &self,
        key: &str,
        options: PutOptions,
    ) -> Result<MultipartUpload> {
        validate_key(key)?;

        let mut request = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
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
            .map_err(|err| from_sdk("create_multipart_upload", err))?;

        let upload_id = response
            .upload_id
            .ok_or_else(|| Error::multipart(key, "R2 did not return an upload ID"))?;

        Ok(MultipartUpload {
            key: key.to_string(),
            upload_id,
            initiated: None,
        })
    }

    /// Uploads one part of an in-progress multipart upload.
    ///
    /// `part_number` is 1-based. Every part except the last must be at least
    /// [`MIN_PART_SIZE`] bytes — and R2 is stricter than S3 here: every part
    /// except the last must also be *exactly the same size*, so pick one part
    /// size and use it throughout. The higher-level
    /// [`multipart_upload_file`](R2Client::multipart_upload_file) already does.
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        body: impl IntoBody,
    ) -> Result<CompletedPart> {
        if part_number < 1 || part_number as u64 > MAX_PARTS {
            return Err(Error::invalid_argument(
                "part_number",
                format!("part number must be between 1 and {MAX_PARTS}, got {part_number}"),
            ));
        }

        let response = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .body(body.into_body())
            .send()
            .await
            .map_err(|err| from_sdk("upload_part", err))?;

        let etag = response
            .e_tag
            .ok_or_else(|| Error::multipart(key, format!("part {part_number} returned no ETag")))?;

        Ok(CompletedPart { part_number, etag })
    }

    /// Finishes a multipart upload, assembling the parts into one object.
    pub async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<PutOutcome> {
        if parts.is_empty() {
            return Err(Error::multipart(
                key,
                "cannot complete an upload with no parts",
            ));
        }

        let mut parts = parts;
        parts.sort_by_key(|part| part.part_number);

        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                parts
                    .into_iter()
                    .map(|part| {
                        SdkCompletedPart::builder()
                            .part_number(part.part_number)
                            .e_tag(part.etag)
                            .build()
                    })
                    .collect(),
            ))
            .build();

        let response = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|err| from_sdk("complete_multipart_upload", err))?;

        Ok(PutOutcome {
            key: key.to_string(),
            etag: response.e_tag,
        })
    }

    /// Cancels a multipart upload and releases the parts already stored.
    pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|err| from_sdk("abort_multipart_upload", err))?;
        Ok(())
    }

    /// Lists multipart uploads that were started but never completed,
    /// following pagination to the end.
    ///
    /// Useful for cleaning up storage held by interrupted uploads. Note this
    /// returns uploads started by anyone with access to the bucket, not just
    /// this process — check the keys before aborting any of them.
    pub async fn list_multipart_uploads(&self) -> Result<Vec<MultipartUpload>> {
        let mut uploads = Vec::new();
        let mut markers: Option<(String, Option<String>)> = None;

        loop {
            let mut request = self.client.list_multipart_uploads().bucket(&self.bucket);
            if let Some((key_marker, upload_id_marker)) = markers {
                request = request
                    .key_marker(key_marker)
                    .set_upload_id_marker(upload_id_marker);
            }

            let response = request
                .send()
                .await
                .map_err(|err| from_sdk("list_multipart_uploads", err))?;

            uploads.extend(
                response
                    .uploads
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|upload| {
                        Some(MultipartUpload {
                            key: upload.key?,
                            upload_id: upload.upload_id?,
                            initiated: upload.initiated,
                        })
                    }),
            );

            // As with object listing, a truncated page without a marker would
            // otherwise re-read page one forever.
            match response.next_key_marker {
                Some(key_marker) if response.is_truncated.unwrap_or(false) => {
                    markers = Some((key_marker, response.next_upload_id_marker));
                }
                _ => break,
            }
        }

        Ok(uploads)
    }
}

/// Reads a local file's size, reporting a missing or unreadable file as
/// [`Error::File`] rather than a bare I/O error with no path in it.
async fn file_size(path: &Path) -> Result<u64> {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.len())
        .map_err(|err| Error::file(path, "could not read the file's metadata", Some(err)))
}

/// Picks a part size that satisfies both the service minimum and the 10,000
/// part ceiling for a file of `size` bytes.
pub(crate) fn effective_part_size(size: u64, requested: u64) -> Result<u64> {
    let mut part_size = requested.max(MIN_PART_SIZE);

    if size.div_ceil(part_size) > MAX_PARTS {
        // Grow just enough that the file fits in MAX_PARTS parts.
        part_size = size.div_ceil(MAX_PARTS);
    }

    if size.div_ceil(part_size) > MAX_PARTS {
        return Err(Error::invalid_argument(
            "part_size",
            format!("file of {size} bytes cannot be split into at most {MAX_PARTS} parts"),
        ));
    }

    Ok(part_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DEFAULT_PART_SIZE;

    #[test]
    fn raises_part_size_to_service_minimum() {
        assert_eq!(
            effective_part_size(100 * 1024 * 1024, 1024).unwrap(),
            MIN_PART_SIZE
        );
    }

    #[test]
    fn keeps_requested_part_size_when_valid() {
        assert_eq!(
            effective_part_size(100 * 1024 * 1024, DEFAULT_PART_SIZE).unwrap(),
            DEFAULT_PART_SIZE
        );
    }

    #[test]
    fn grows_part_size_to_stay_under_the_part_ceiling() {
        // 1 TiB at the 5 MiB minimum would need ~210k parts.
        let size = 1024 * 1024 * 1024 * 1024;
        let part_size = effective_part_size(size, MIN_PART_SIZE).unwrap();
        assert!(part_size > MIN_PART_SIZE);
        assert!(size.div_ceil(part_size) <= MAX_PARTS);
    }

    #[test]
    fn part_count_covers_the_whole_file_exactly() {
        let size = 100 * 1024 * 1024 + 1;
        let part_size = effective_part_size(size, DEFAULT_PART_SIZE).unwrap();
        let count = size.div_ceil(part_size);
        let last = size - (count - 1) * part_size;
        assert!(last > 0 && last <= part_size);
        assert_eq!((count - 1) * part_size + last, size);
    }
}
