use anyhow::Result;
use aws_sdk_s3::Client;

/// S3 client wrapper around `aws-sdk-s3`.
///
/// Supports both AWS S3 and S3-compatible services (e.g. Cloudflare R2)
/// via the optional custom `endpoint` URL.
pub struct S3Client {
    client: Client,
    bucket: String,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

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
}
