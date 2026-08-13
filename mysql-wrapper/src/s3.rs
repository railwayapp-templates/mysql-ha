//! Thin S3-compatible bucket client for PITR archiving/restore.
//!
//! Every credential and endpoint comes straight from the env contract (see
//! config.rs) — never ambient AWS config/credentials/profile files, since
//! this wrapper has no business picking up whatever IAM environment happens
//! to be lying around the host. `force_path_style(true)` because the env
//! contract's `BINLOG_ARCHIVE_ENDPOINT`/`BINLOG_RECOVER_FROM_ENDPOINT` may
//! point at any S3-compatible provider, most of which don't support
//! virtual-hosted-style addressing.

use crate::pitr::S3Location;
use anyhow::{anyhow, Context, Result};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Multipart part size for the unbounded-length archive stream (mysqldump
/// piped through gzip has no known Content-Length up front, which plain
/// PutObject requires) — 8MB keeps the in-flight buffer small while staying
/// comfortably above S3's 5MB minimum part size for every part but the last.
const MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;

#[derive(Clone)]
pub struct S3Client {
    client: Client,
    bucket: String,
}

impl S3Client {
    pub async fn new(location: &S3Location) -> Result<Self> {
        let credentials = Credentials::new(
            &location.access_key,
            &location.secret_key,
            None,
            None,
            "railway-mysql-ha-pitr",
        );
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(Region::new(location.region.clone()))
            .endpoint_url(&location.endpoint)
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();
        Ok(Self {
            client: Client::from_conf(config),
            bucket: location.bucket.clone(),
        })
    }

    /// Does this key exist? Used both for the "has a full ever been taken"
    /// check and the startup HEAD-verification pass over the local uploaded-
    /// binlog state file.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(e)) if matches!(e.err(), HeadObjectError::NotFound(_)) => {
                Ok(false)
            }
            Err(e) => Err(anyhow::Error::new(e).context(format!("HEAD {key}"))),
        }
    }

    /// Every key under `prefix`, paginated.
    pub async fn list_keys_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut pages = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = pages.next().await {
            let page = page.with_context(|| format!("listing objects under {prefix}"))?;
            for obj in page.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
        }
        Ok(keys)
    }

    pub async fn get_object_bytes(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("GET {key}"))?;
        let bytes = resp
            .body
            .collect()
            .await
            .with_context(|| format!("reading body of {key}"))?
            .into_bytes();
        Ok(bytes.to_vec())
    }

    /// The object's body as a plain `AsyncRead`, for relaying straight into a
    /// subprocess's stdin (the restore path's `gunzip -c | mysql` load)
    /// without buffering the whole object in memory first.
    pub async fn get_object_async_read(&self, key: &str) -> Result<impl AsyncRead + Unpin> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("GET {key}"))?;
        Ok(resp.body.into_async_read())
    }

    /// Stream an object straight to a local file (used for binlogs, which
    /// `mysqlbinlog` needs as real files on disk).
    pub async fn download_to_file(&self, key: &str, dest: &Path) -> Result<()> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("GET {key}"))?;
        let mut reader = resp.body.into_async_read();
        let mut file = tokio::fs::File::create(dest)
            .await
            .with_context(|| format!("creating {}", dest.display()))?;
        tokio::io::copy(&mut reader, &mut file)
            .await
            .with_context(|| format!("writing {}", dest.display()))?;
        Ok(())
    }

    /// Upload a local file whose size is known up front (a closed binlog
    /// file) as a single PutObject.
    pub async fn put_object_from_file(&self, key: &str, path: &Path) -> Result<()> {
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("opening {}", path.display()))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("PUT {key}"))?;
        Ok(())
    }

    pub async fn put_object_bytes(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .with_context(|| format!("PUT {key}"))?;
        Ok(())
    }

    /// Stream an unbounded-length source (the gzip'd mysqldump pipe) to S3
    /// via a multipart upload — plain PutObject needs a Content-Length that a
    /// live subprocess pipe can't provide ahead of time. Aborts the upload on
    /// any failure so a partial dump never lingers as an incomplete-but-
    /// billed multipart upload.
    pub async fn upload_multipart(
        &self,
        key: &str,
        mut src: impl AsyncRead + Unpin,
    ) -> Result<()> {
        let create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("create_multipart_upload {key}"))?;
        let upload_id = create
            .upload_id()
            .context("create_multipart_upload returned no upload id")?
            .to_string();

        match self.upload_parts(key, &upload_id, &mut src).await {
            Ok(parts) if !parts.is_empty() => {
                let completed = CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build();
                self.client
                    .complete_multipart_upload()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .multipart_upload(completed)
                    .send()
                    .await
                    .with_context(|| format!("complete_multipart_upload {key}"))?;
                Ok(())
            }
            Ok(_empty) => {
                // Nothing was ever read from the source — abort the
                // multipart (S3 refuses to complete one with zero parts) and
                // fall back to an ordinary empty PutObject.
                self.abort_multipart(key, &upload_id).await;
                self.put_object_bytes(key, Vec::new()).await
            }
            Err(e) => {
                self.abort_multipart(key, &upload_id).await;
                Err(e)
            }
        }
    }

    async fn abort_multipart(&self, key: &str, upload_id: &str) {
        if let Err(e) = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
        {
            tracing::warn!(error = %e, key, "could not abort a failed multipart upload");
        }
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        src: &mut (impl AsyncRead + Unpin),
    ) -> Result<Vec<CompletedPart>> {
        let mut parts = Vec::new();
        let mut part_number: i32 = 1;
        loop {
            let mut buf = vec![0u8; MULTIPART_PART_SIZE];
            let mut filled = 0usize;
            while filled < buf.len() {
                let n = src
                    .read(&mut buf[filled..])
                    .await
                    .context("reading source stream")?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }
            buf.truncate(filled);
            let resp = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buf))
                .send()
                .await
                .with_context(|| format!("upload_part {part_number} for {key}"))?;
            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .set_e_tag(resp.e_tag().map(str::to_string))
                    .build(),
            );
            part_number = part_number
                .checked_add(1)
                .ok_or_else(|| anyhow!("multipart upload exceeded the maximum part count"))?;
        }
        Ok(parts)
    }
}
