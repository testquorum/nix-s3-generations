use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand};
use futures::stream::TryStreamExt;

use crate::narinfo;
use crate::prune::{DEFAULT_FRESHNESS_BUFFER_SECS, MARK_CONCURRENCY, is_not_found};
use crate::s3::{S3Client, S3Object};
use crate::s3_keys;

#[derive(Args, Debug, Clone)]
pub struct CheckArgs {
    /// Restrict the run to a single check. With no subcommand, every
    /// registered check runs in sequence.
    #[command(subcommand)]
    pub command: Option<CheckSubcommand>,

    #[arg(long)]
    pub bucket: String,

    #[arg(long, default_value = "us-east-1")]
    pub region: String,

    #[arg(long)]
    pub endpoint: Option<String>,

    /// Repair findings where it's safe to do so. Default: report-only.
    /// `check` is diagnostic — running it must never mutate state by
    /// accident.
    #[arg(long, default_value = "false")]
    pub fix: bool,

    /// Skip narinfos whose `LastModified` is within this many seconds of
    /// the run start. A concurrent push on another shard may have written
    /// the narinfo before the NAR is HEAD-visible; without this filter we
    /// would flag it (and with `--fix`, delete it).
    #[arg(long, default_value_t = DEFAULT_FRESHNESS_BUFFER_SECS)]
    pub freshness_buffer_secs: u64,
}

#[derive(Subcommand, Debug, Clone)]
pub enum CheckSubcommand {
    /// Walk every `.narinfo` in the bucket and verify the file its URL
    /// points to exists. With `--fix`, delete narinfos whose in-bucket
    /// NAR is missing — `nix copy` treats the narinfo as authoritative
    /// and won't repair this case on its own.
    InvalidNarinfoUrls,
}

pub async fn run(args: CheckArgs) -> Result<()> {
    let s3 = S3Client::new(
        args.bucket.clone(),
        args.region.clone(),
        args.endpoint.clone(),
    )
    .await?;

    let cutoff = Utc::now() - Duration::seconds(args.freshness_buffer_secs as i64);

    tracing::info!(
        bucket = %args.bucket,
        region = %args.region,
        fix = args.fix,
        freshness_buffer_secs = args.freshness_buffer_secs,
        cutoff = %cutoff,
        command = ?args.command,
        "starting check",
    );

    match args.command {
        None => {
            let stats = check_invalid_narinfo_urls(&s3, cutoff, args.fix).await?;
            log_invalid_narinfo_url_stats(&stats, args.fix);
        }
        Some(CheckSubcommand::InvalidNarinfoUrls) => {
            let stats = check_invalid_narinfo_urls(&s3, cutoff, args.fix).await?;
            log_invalid_narinfo_url_stats(&stats, args.fix);
        }
    }

    tracing::info!("check complete");
    Ok(())
}

/// Where a narinfo's `URL:` field points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlClass {
    /// `nar/<hash>.nar[.<ext>]` shape — a key inside this bucket.
    Bucket,
    /// Absolute URL (contains `://`) — points outside this bucket.
    External,
    /// Neither absolute nor `is_nar_key`-shaped: nothing this tool can
    /// safely act on. Includes empty strings, paths with embedded `/`s
    /// after `nar/`, leading-`/` absolute paths, etc.
    Malformed,
}

/// Classify a NARinfo `URL:` value.
///
/// `is_nar_key` already enforces the documented `nar/<hash>.nar[.<ext>]`
/// schema; we layer the absolute-URL check on top of it because
/// `is_nar_key` would reject e.g. `https://other-cache/nar/x.nar.xz` as
/// Malformed when it's really an externally-hosted NAR we don't own.
pub fn classify_url(url: &str) -> UrlClass {
    if url.contains("://") {
        UrlClass::External
    } else if s3_keys::is_nar_key(url) {
        UrlClass::Bucket
    } else {
        UrlClass::Malformed
    }
}

#[derive(Debug, Default)]
pub struct InvalidNarinfoUrlStats {
    pub narinfos_checked: usize,
    pub ok: usize,
    pub missing_nar: usize,
    pub external_url: usize,
    pub malformed_url: usize,
    pub unparseable: usize,
    pub deleted: usize,
    pub bytes_deleted: u64,
}

/// One classification result for a single narinfo.
#[derive(Debug)]
enum Finding {
    /// URL is in-bucket and the NAR exists.
    Ok,
    /// URL is in-bucket but the NAR is missing — the fixable case.
    MissingNar {
        narinfo_key: String,
        nar_url: String,
        bytes: u64,
    },
    /// URL is an absolute http(s):// URL — not ours to verify.
    ExternalUrl { narinfo_key: String, url: String },
    /// URL is neither absolute nor `is_nar_key`-shaped — anomalous.
    MalformedUrl { narinfo_key: String, url: String },
    /// Body is non-UTF-8 or fails `parse_narinfo`. Reported but not
    /// auto-fixed: the operator should inspect before deleting.
    Unparseable { narinfo_key: String },
    /// Narinfo 404'd between list and GET. Race with another run; ignore.
    Vanished,
}

/// Walk every narinfo in the bucket and verify each URL resolves.
///
/// Pipeline mirrors `prune::sweep_nars`: list → filter (narinfo-shape +
/// guardrails + freshness) → fan out GET + URL classification + HEAD via
/// `try_buffer_unordered` → collect findings → optional `--fix` delete
/// pass.
///
/// Errors only on non-404 transport/parse failures during listing or HEAD.
/// Parse failures on individual narinfo bodies are surfaced as
/// `Unparseable` findings, not aborts — diagnosing those is *the* job of
/// this command. `sweep_nars` aborts on parse failure because it would
/// risk deleting live NARs; here we delete nothing without `--fix`, so
/// reporting is the right answer.
pub async fn check_invalid_narinfo_urls(
    s3: &S3Client,
    cutoff: DateTime<Utc>,
    fix: bool,
) -> Result<InvalidNarinfoUrlStats> {
    tracing::info!("listing all objects in bucket for narinfo URL check");

    let objects = s3
        .list_objects("")
        .try_filter(|o| futures::future::ready(s3_keys::is_narinfo_key(&o.key)))
        .try_filter(|o| {
            futures::future::ready(
                !o.key.starts_with("generations/") && o.key.as_str() != "nix-cache-info",
            )
        })
        .try_filter(|o| {
            futures::future::ready(match o.last_modified {
                Some(lm) if lm < cutoff => true,
                Some(lm) => {
                    tracing::info!(
                        key = o.key.as_str(),
                        last_modified = %lm,
                        cutoff = %cutoff,
                        "skipping recently-modified narinfo (freshness filter)"
                    );
                    false
                }
                None => {
                    tracing::warn!(
                        key = o.key.as_str(),
                        "narinfo has no LastModified in listing, skipping defensively"
                    );
                    false
                }
            })
        });

    let findings: Vec<Finding> = objects
        .map_ok(|obj| classify_one(s3, obj))
        .try_buffer_unordered(MARK_CONCURRENCY)
        .try_collect()
        .await?;

    let mut stats = InvalidNarinfoUrlStats::default();
    let mut to_delete: Vec<(String, u64)> = Vec::new();

    for finding in findings {
        match finding {
            Finding::Vanished => {
                // Don't count — the narinfo isn't there anymore.
            }
            Finding::Ok => {
                stats.narinfos_checked += 1;
                stats.ok += 1;
            }
            Finding::MissingNar {
                narinfo_key,
                nar_url,
                bytes,
            } => {
                stats.narinfos_checked += 1;
                stats.missing_nar += 1;
                tracing::warn!(
                    narinfo = narinfo_key.as_str(),
                    nar_url = nar_url.as_str(),
                    "narinfo references a NAR that does not exist in this bucket"
                );
                to_delete.push((narinfo_key, bytes));
            }
            Finding::ExternalUrl { narinfo_key, url } => {
                stats.narinfos_checked += 1;
                stats.external_url += 1;
                tracing::info!(
                    narinfo = narinfo_key.as_str(),
                    url = url.as_str(),
                    "narinfo URL is absolute; assuming external cache and skipping"
                );
            }
            Finding::MalformedUrl { narinfo_key, url } => {
                stats.narinfos_checked += 1;
                stats.malformed_url += 1;
                tracing::warn!(
                    narinfo = narinfo_key.as_str(),
                    url = url.as_str(),
                    "narinfo URL is neither an absolute URL nor a valid `nar/<hash>.nar[.<ext>]` key"
                );
            }
            Finding::Unparseable { narinfo_key } => {
                stats.narinfos_checked += 1;
                stats.unparseable += 1;
                tracing::warn!(
                    narinfo = narinfo_key.as_str(),
                    "narinfo body is non-UTF-8 or failed to parse; not auto-fixed"
                );
            }
        }
    }

    if fix {
        for (key, bytes) in &to_delete {
            tracing::info!(
                key = key.as_str(),
                bytes,
                "deleting narinfo with missing NAR"
            );
            match s3.delete_object(key).await {
                Ok(_) => {
                    stats.deleted += 1;
                    stats.bytes_deleted += bytes;
                }
                Err(e) => {
                    tracing::error!(
                        key = key.as_str(),
                        error = %e,
                        "failed to delete narinfo"
                    );
                }
            }
        }
        if stats.deleted > 0 {
            tracing::info!(
                "deleted {} broken narinfo(s); re-run `nix-s3-generations prune narinfo` \
                 to clean up any narinfos exposed as unreachable by these deletions",
                stats.deleted
            );
        }
    } else if !to_delete.is_empty() {
        tracing::info!(
            "would delete {} broken narinfo(s); re-run with --fix to repair",
            to_delete.len()
        );
    }

    Ok(stats)
}

async fn classify_one(s3: &S3Client, obj: S3Object) -> Result<Finding> {
    let key = obj.key;
    let bytes = obj.size.unwrap_or(0).max(0) as u64;

    let body = match s3.get_object(&key).await {
        Ok(b) => b,
        Err(e) if is_not_found(&e) => return Ok(Finding::Vanished),
        Err(e) => {
            return Err(e.context(format!("failed to fetch narinfo {key} during check")));
        }
    };

    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(_) => return Ok(Finding::Unparseable { narinfo_key: key }),
    };

    let info = match narinfo::parse_narinfo(body_str) {
        Ok(i) => i,
        Err(_) => return Ok(Finding::Unparseable { narinfo_key: key }),
    };

    match classify_url(&info.url) {
        UrlClass::External => Ok(Finding::ExternalUrl {
            narinfo_key: key,
            url: info.url,
        }),
        UrlClass::Malformed => Ok(Finding::MalformedUrl {
            narinfo_key: key,
            url: info.url,
        }),
        UrlClass::Bucket => {
            let exists = s3
                .object_exists(&info.url)
                .await
                .with_context(|| format!("HEAD {} (referenced by {key})", info.url))?;
            if exists {
                Ok(Finding::Ok)
            } else {
                Ok(Finding::MissingNar {
                    narinfo_key: key,
                    nar_url: info.url,
                    bytes,
                })
            }
        }
    }
}

fn log_invalid_narinfo_url_stats(stats: &InvalidNarinfoUrlStats, fix: bool) {
    tracing::info!(
        narinfos_checked = stats.narinfos_checked,
        ok = stats.ok,
        missing_nar = stats.missing_nar,
        external_url = stats.external_url,
        malformed_url = stats.malformed_url,
        unparseable = stats.unparseable,
        deleted = stats.deleted,
        bytes_deleted = stats.bytes_deleted,
        fix,
        "invalid-narinfo-urls check complete",
    );
    if stats.external_url > 0 {
        tracing::warn!(
            count = stats.external_url,
            "narinfos with external URLs were not checked; verify these are intended"
        );
    }
    if stats.malformed_url > 0 {
        tracing::warn!(
            count = stats.malformed_url,
            "narinfos with malformed URLs need operator attention; not auto-fixed"
        );
    }
    if stats.unparseable > 0 {
        tracing::warn!(
            count = stats.unparseable,
            "unparseable narinfos need operator attention; not auto-fixed"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use chrono::TimeZone;

    fn mock_client(events: Vec<ReplayEvent>) -> S3Client {
        let http_client = StaticReplayClient::new(events);
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .region(Region::new("us-east-1"))
            .http_client(http_client.clone())
            .endpoint_url("http://localhost:1234")
            .build();
        let client = aws_sdk_s3::Client::from_conf(config);
        S3Client::from_sdk_client("test-bucket".to_string(), client)
    }

    fn empty_request() -> http::Request<SdkBody> {
        http::Request::builder()
            .uri("http://localhost:1234/")
            .body(SdkBody::empty())
            .unwrap()
    }

    const OLD_LAST_MODIFIED: &str = "2020-01-01T00:00:00.000Z";
    const DEFAULT_SIZE: u64 = 100;

    fn list_xml(keys: &[&str]) -> String {
        let contents: String = keys
            .iter()
            .map(|k| {
                format!(
                    "  <Contents><Key>{k}</Key><LastModified>{OLD_LAST_MODIFIED}</LastModified><Size>{DEFAULT_SIZE}</Size></Contents>\n"
                )
            })
            .collect();
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
{contents}</ListBucketResult>"#
        )
    }

    fn list_xml_with_times(items: &[(&str, &str)]) -> String {
        let contents: String = items
            .iter()
            .map(|(k, ts)| {
                format!("  <Contents><Key>{k}</Key><LastModified>{ts}</LastModified><Size>{DEFAULT_SIZE}</Size></Contents>\n")
            })
            .collect();
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
{contents}</ListBucketResult>"#
        )
    }

    fn list_xml_with_sizes(items: &[(&str, u64)]) -> String {
        let contents: String = items
            .iter()
            .map(|(k, sz)| {
                format!(
                    "  <Contents><Key>{k}</Key><LastModified>{OLD_LAST_MODIFIED}</LastModified><Size>{sz}</Size></Contents>\n"
                )
            })
            .collect();
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
{contents}</ListBucketResult>"#
        )
    }

    fn narinfo_body(hash: &str) -> String {
        format!(
            "StorePath: /nix/store/{hash}-pkg\n\
             URL: nar/{hash}.nar.xz\n\
             References:\n"
        )
    }

    fn default_cutoff() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    // ── classify_url ────────────────────────────────────────────────

    #[test]
    fn classify_url_table() {
        let cases = [
            ("nar/abc.nar.xz", UrlClass::Bucket),
            ("nar/abc.nar", UrlClass::Bucket),
            ("nar/abc.nar.zst", UrlClass::Bucket),
            ("https://other-cache/nar/abc.nar.xz", UrlClass::External),
            ("http://other/abc.nar", UrlClass::External),
            ("s3://bucket/abc.nar.xz", UrlClass::External),
            ("nar/foo/bar.nar.xz", UrlClass::Malformed),
            ("/abs/path", UrlClass::Malformed),
            ("abc.nar.xz", UrlClass::Malformed),
            ("", UrlClass::Malformed),
        ];
        for (url, expected) in cases {
            assert_eq!(
                classify_url(url),
                expected,
                "classify_url({url:?}) mismatch"
            );
        }
    }

    // ── pipeline tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn happy_path_nar_exists() {
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let client = mock_client(vec![
            // LIST ""
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[&format!("{hash}.narinfo")])))
                    .unwrap(),
            ),
            // GET narinfo
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(hash)))
                    .unwrap(),
            ),
            // HEAD nar/<hash>.nar.xz → 200
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 1);
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.missing_nar, 0);
        assert_eq!(stats.deleted, 0);
    }

    #[tokio::test]
    async fn missing_nar_reports_without_fix() {
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[&format!("{hash}.narinfo")])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(hash)))
                    .unwrap(),
            ),
            // HEAD → 404
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(404)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // No DELETE — StaticReplayClient panics on extra requests.
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 1);
        assert_eq!(stats.missing_nar, 1);
        assert_eq!(stats.ok, 0);
        assert_eq!(stats.deleted, 0);
        assert_eq!(stats.bytes_deleted, 0);
    }

    #[tokio::test]
    async fn missing_nar_with_fix_deletes_narinfo() {
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml_with_sizes(&[(
                        &format!("{hash}.narinfo"),
                        4321,
                    )])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(hash)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(404)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // DELETE narinfo
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), true)
            .await
            .unwrap();
        assert_eq!(stats.missing_nar, 1);
        assert_eq!(stats.deleted, 1);
        assert_eq!(stats.bytes_deleted, 4321);
    }

    #[tokio::test]
    async fn freshness_filter_skips_recent_narinfo() {
        // Narinfo's LastModified is *after* the cutoff: skipped entirely,
        // no GET issued. StaticReplayClient panics on extra requests.
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(list_xml_with_times(&[(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                    "2026-05-03T12:00:00.000Z",
                )])))
                .unwrap(),
        )]);

        let cutoff = Utc.with_ymd_and_hms(2026, 5, 3, 11, 0, 0).unwrap();
        let stats = check_invalid_narinfo_urls(&client, cutoff, true)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 0);
        assert_eq!(stats.missing_nar, 0);
        assert_eq!(stats.deleted, 0);
    }

    #[tokio::test]
    async fn unparseable_narinfo_is_reported_not_deleted() {
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                    ])))
                    .unwrap(),
            ),
            // Body parses fine as UTF-8 but isn't a narinfo.
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from("not a narinfo"))
                    .unwrap(),
            ),
            // No HEAD, no DELETE — would panic.
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), true)
            .await
            .unwrap();
        assert_eq!(stats.unparseable, 1);
        assert_eq!(stats.deleted, 0);
    }

    #[tokio::test]
    async fn guardrails_skip_generations_and_cache_info() {
        // generations/.../*.narinfo and nix-cache-info must not be touched
        // by this check, even though `is_narinfo_key` matches the former.
        // The "nix-cache-info" listing entry should also be filtered
        // (is_narinfo_key returns false for it, but tests pin behavior).
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(list_xml(&[
                    "generations/dom/1/odd.narinfo",
                    "nix-cache-info",
                ])))
                .unwrap(),
        )]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), true)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 0);
    }

    #[tokio::test]
    async fn external_url_is_skipped() {
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(
                        "StorePath: /nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg\n\
                         URL: https://other-cache.example/nar/abc.nar.xz\n\
                         References:\n",
                    ))
                    .unwrap(),
            ),
            // No HEAD — external URLs are not verified.
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), true)
            .await
            .unwrap();
        assert_eq!(stats.external_url, 1);
        assert_eq!(stats.deleted, 0);
    }

    #[tokio::test]
    async fn vanished_narinfo_is_silently_skipped() {
        // Narinfo listed but GET 404s — race with another deletion. Not
        // a finding, not counted.
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(404)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = check_invalid_narinfo_urls(&client, default_cutoff(), true)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 0);
    }
}
