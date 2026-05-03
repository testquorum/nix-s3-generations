use anyhow::Result;
use async_stream::stream;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};
use futures::Stream;

/// S3 client wrapper around `aws-sdk-s3`.
///
/// Supports both AWS S3 and S3-compatible services (e.g. Cloudflare R2)
/// via the optional custom `endpoint` URL.
pub struct S3Client {
    client: Client,
    bucket: String,
}

/// One entry from `list_objects`. `last_modified` is the server-side
/// `LastModified` reported by S3; `None` only if the listing omits it
/// (shouldn't happen for `list_objects_v2`, but callers must be defensive).
#[derive(Debug, Clone)]
pub struct S3Object {
    pub key: String,
    pub last_modified: Option<DateTime<Utc>>,
}

impl S3Client {
    /// Create a new `S3Client` from environment credentials.
    ///
    /// Reads `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` from the
    /// environment via `aws-config`.  An optional `endpoint` overrides
    /// the default S3 endpoint (useful for R2, MinIO, etc.).
    pub async fn new(bucket: String, region: String, endpoint: Option<String>) -> Result<Self> {
        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region))
            .load()
            .await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&shared_config);
        if let Some(url) = endpoint {
            // Nix's S3 substituter URI accepts a bare host
            // (`endpoint=foo.r2.cloudflarestorage.com`); the AWS SDK's
            // `endpoint_url` requires a full URI. Add a scheme if missing.
            let url = if url.contains("://") {
                url
            } else {
                format!("https://{}", url)
            };
            s3_config = s3_config.endpoint_url(url);
        }

        let client = Client::from_conf(s3_config.build());
        Ok(Self { client, bucket })
    }

    /// Build an `S3Client` from an existing `aws_sdk_s3::Client`.
    /// Used by tests to inject a mocked HTTP client.
    #[cfg(test)]
    pub fn from_sdk_client(bucket: String, client: Client) -> Self {
        Self { client, bucket }
    }

    /// PUT an object with the given bytes.
    pub async fn upload_object(
        &self,
        key: &str,
        data: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<()> {
        let mut builder = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into());

        if let Some(ct) = content_type {
            builder = builder.content_type(ct);
        }

        builder.send().await?;
        Ok(())
    }

    /// HEAD an object. Returns `false` if the object does not exist (404).
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                if let Some(se) = e.as_service_error()
                    && se.is_not_found()
                {
                    return Ok(false);
                }
                Err(e.into())
            }
        }
    }

    /// List objects with the given prefix. Uses pagination via `into_paginator()`.
    /// Each entry includes the server-side `LastModified` so callers (notably
    /// the prune sweep) can filter on object age without a follow-up HEAD.
    pub fn list_objects(&self, prefix: &str) -> impl Stream<Item = Result<S3Object>> {
        let paginator = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator();

        let mut pages = paginator.send();

        stream! {
            loop {
                match pages.next().await {
                    Some(Ok(output)) => {
                        for obj in output.contents() {
                            if let Some(key) = obj.key() {
                                yield Ok(S3Object {
                                    key: key.to_string(),
                                    last_modified: obj.last_modified().and_then(smithy_to_chrono),
                                });
                            }
                        }
                    }
                    Some(Err(e)) => {
                        yield Err(e.into());
                        break;
                    },
                    None => break,
                }
            }
        }
    }

    /// Get an object by key. Returns the object body as bytes.
    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let data = output.body.collect().await?.into_bytes().to_vec();
        Ok(data)
    }

    /// Delete an object by key.
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }
}

/// Convert the AWS SDK's `DateTime` (S3's object-level `LastModified` from
/// the `ListObjectsV2` response) into a `chrono::DateTime<Utc>`. Returns
/// `None` if the value is outside chrono's representable range — which it
/// never is for real S3 timestamps.
fn smithy_to_chrono(dt: &aws_sdk_s3::primitives::DateTime) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(dt.secs(), dt.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use futures::TryStreamExt;

    /// Helper to create an `S3Client` wired to a `StaticReplayClient`.
    fn mock_client(events: Vec<ReplayEvent>) -> S3Client {
        let http_client = StaticReplayClient::new(events);
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .region(Region::new("us-east-1"))
            .http_client(http_client.clone())
            .endpoint_url("http://localhost:1234")
            .build();
        let client = Client::from_conf(config);
        S3Client::from_sdk_client("test-bucket".to_string(), client)
    }

    fn empty_request() -> http::Request<SdkBody> {
        http::Request::builder()
            .uri("http://localhost:1234/")
            .body(SdkBody::empty())
            .unwrap()
    }

    #[tokio::test]
    async fn test_upload_object_success() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        client
            .upload_object("test-key", b"hello".to_vec(), Some("text/plain"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_upload_object_without_content_type() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        client
            .upload_object("test-key", b"data".to_vec(), None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_object_exists_true() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        assert!(client.object_exists("test-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_exists_false_on_404() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(404)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        assert!(!client.object_exists("missing-key").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_exists_propagates_server_error() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(500)
                .body(SdkBody::from("internal error"))
                .unwrap(),
        )]);

        let result = client.object_exists("test-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_objects_single_page() {
        let response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>prefix/key1</Key>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
  </Contents>
  <Contents>
    <Key>prefix/key2</Key>
    <LastModified>2026-01-02T00:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(response_body))
                .unwrap(),
        )]);

        let objects: Vec<S3Object> = client.list_objects("prefix/").try_collect().await.unwrap();
        let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(keys, vec!["prefix/key1", "prefix/key2"]);
        assert!(objects[0].last_modified.is_some());
        assert!(objects[1].last_modified.is_some());
        assert!(objects[0].last_modified.unwrap() < objects[1].last_modified.unwrap());
    }

    #[tokio::test]
    async fn test_list_objects_empty_bucket() {
        let response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
</ListBucketResult>"#;
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(response_body))
                .unwrap(),
        )]);

        let objects: Vec<S3Object> = client.list_objects("prefix/").try_collect().await.unwrap();
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn test_list_objects_pagination() {
        let response_body_page1 = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>prefix/key1</Key>
    <LastModified>2026-01-01T00:00:00.000Z</LastModified>
  </Contents>
  <NextContinuationToken>token123</NextContinuationToken>
</ListBucketResult>"#;
        let response_body_page2 = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents>
    <Key>prefix/key2</Key>
    <LastModified>2026-01-02T00:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(response_body_page1))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(response_body_page2))
                    .unwrap(),
            ),
        ]);

        let objects: Vec<S3Object> = client.list_objects("prefix/").try_collect().await.unwrap();
        let keys: Vec<&str> = objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(keys, vec!["prefix/key1", "prefix/key2"]);
    }

    #[tokio::test]
    async fn test_get_object_success() {
        let response_body = "hello world";
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(response_body))
                .unwrap(),
        )]);

        let data = client.get_object("test-key").await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn test_get_object_not_found() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(404)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        let result = client.get_object("missing-key").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_object_success() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .unwrap(),
        )]);

        client.delete_object("test-key").await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_object_error() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(500)
                .body(SdkBody::from("internal error"))
                .unwrap(),
        )]);

        let result = client.delete_object("test-key").await;
        assert!(result.is_err());
    }
}
