use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Args;

use crate::coordination::{self, AttemptStatus, PruneAttempt, PushConflict};
use crate::generation::GenerationRoot;
use crate::nix::{self, NixStorePath, StorePath};
use crate::prune;
use crate::s3::S3Client;
use crate::s3_keys;

/// How often to re-fetch a running prune-attempt marker while waiting for
/// it to flip `done`. Bounded by the prune's conflict-poll cadence —
/// pushers are unblocked one or two `POLL_INTERVAL`s after the prune sees
/// their conflict notice.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Validate that a shard id is safe to use as the basename of a generation
/// entry: non-empty, no `/` (so it can't introduce extra path levels), and
/// not `.`/`..` (which would resolve weirdly even though S3 keys are flat).
fn validate_shard_id(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("shard-id must not be empty".to_string());
    }
    if s.contains('/') {
        return Err(format!("shard-id must not contain '/': {s:?}"));
    }
    if s == "." || s == ".." {
        return Err(format!("shard-id must not be '.' or '..': {s:?}"));
    }
    Ok(s.to_string())
}

#[derive(Args, Debug, Clone)]
pub struct PushArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long, default_value = "us-east-1")]
    pub region: String,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(long)]
    pub snapshot_before: PathBuf,

    /// The generational-GC domain this push belongs to. Generations are
    /// numbered and aged out independently per domain. May contain `/` to
    /// namespace under repo/workflow/etc.
    #[arg(long)]
    pub domain: String,

    /// Numeric generation within the domain. Multiple pushes (e.g. matrix
    /// legs) can share a generation number; the GC treats one generation
    /// as a single unit.
    #[arg(long)]
    pub generation: u64,

    /// Identifier for this push within the generation. Forms the leaf
    /// filename (`<shard-id>.json`), so it must not contain `/`. Defaults to
    /// `gen` for callers with a single push per generation.
    #[arg(long, default_value = "gen", value_parser = validate_shard_id)]
    pub shard_id: String,
}

/// Construct the `s3://` URI used for re-running `nix copy` after a prune
/// races us. Mirrors the form built by the action's post-build-hook script
/// minus the `&secret-key=...` query — paths uploaded by the post-build
/// hook already carry signatures in the local nix store, and `nix copy`
/// preserves them rather than re-signing. Re-running as the runner user
/// (without root key access) is therefore fine.
fn build_copy_uri(bucket: &str, region: &str, endpoint: Option<&str>) -> String {
    let is_native_aws = matches!(endpoint, Some("s3.amazonaws.com") | None)
        || endpoint.is_some_and(|e| {
            // Mirror buildNixS3Params's regex: s3.{region}.amazonaws.com
            e.starts_with("s3.") && e.ends_with(".amazonaws.com") && e.matches('.').count() == 3
        });
    if is_native_aws {
        return format!("s3://{bucket}?region={region}");
    }
    let endpoint = endpoint.expect("non-native endpoint must be set");
    format!("s3://{bucket}?endpoint={endpoint}&region={region}")
}

/// Shell out to `nix copy --to <uri> <closure_root>`. Idempotent: skips
/// paths the destination already has (HEAD on narinfo). After a prune
/// raced and deleted some of our paths, re-running picks up exactly those.
fn re_run_nix_copy(closure_root: &NixStorePath, copy_uri: &str) -> Result<()> {
    tracing::info!(
        closure_root = %closure_root,
        copy_uri = copy_uri,
        "re-running nix copy after prune raced our push"
    );
    let output = Command::new("nix")
        .arg("--extra-experimental-features")
        .arg("nix-command flakes")
        .arg("copy")
        .arg("--to")
        .arg(copy_uri)
        .arg(closure_root.as_str())
        .output()
        .context("failed to invoke nix copy for post-conflict re-upload")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "nix copy re-run failed with exit {:?}: {}",
            output.status.code(),
            stderr.trim()
        );
    }
    Ok(())
}

/// Upload a `PushConflict` notice into the attempt's `conflicts/` prefix.
/// The prune sees it on its next conflict poll and subtracts the claim.
async fn upload_conflict(
    s3: &S3Client,
    attempt_id: &str,
    push_id: &str,
    claim: &HashSet<String>,
) -> Result<()> {
    let conflict = PushConflict::new(push_id.to_string(), claim.iter().cloned().collect());
    let key = s3_keys::prune_conflict_key(attempt_id, push_id);
    let body = serde_json::to_vec(&conflict).context("failed to serialize push conflict")?;
    s3.upload_object(&key, body, Some("application/json"))
        .await
        .with_context(|| format!("failed to upload conflict notice {key}"))
}

/// Poll the marker for `attempt_id` until its status is `Done`.
///
/// If the marker is missing (e.g. janitor-GC'd between iterations), treat
/// that as `Done` — the attempt is no longer running, which is what the
/// caller cared about.
async fn wait_for_attempt_done(
    s3: &S3Client,
    attempt_id: &str,
    poll_interval: Duration,
) -> Result<()> {
    let key = s3_keys::prune_attempt_key(attempt_id);
    loop {
        match prune::fetch_attempt_marker(s3, &key).await? {
            Some(a) if a.status == AttemptStatus::Done => return Ok(()),
            Some(_) => {
                tokio::time::sleep(poll_interval).await;
            }
            None => {
                tracing::warn!(
                    attempt_id = %attempt_id,
                    "attempt marker missing while waiting; treating as done"
                );
                return Ok(());
            }
        }
    }
}

/// Pre-push coordination: any *running* prune attempt whose `delete_set`
/// overlaps our claim gets a conflict notice; we wait for each to
/// finalize before proceeding. The prune sees the conflict on its next
/// poll, subtracts our hashes, and finalizes the current attempt — at
/// which point our wait returns. After every unblock we re-list, since a
/// freshly-spawned successor attempt may still overlap (the prune may be
/// processing other dead hashes that happen to share our closure).
async fn pre_check_loop(
    s3: &S3Client,
    push_id: &str,
    claim: &HashSet<String>,
    poll_interval: Duration,
) -> Result<()> {
    loop {
        let attempts = prune::list_running_attempts(s3).await?;
        let overlapping: Vec<&PruneAttempt> =
            attempts.iter().filter(|a| has_overlap(a, claim)).collect();
        if overlapping.is_empty() {
            return Ok(());
        }
        for a in overlapping {
            tracing::info!(
                attempt_id = %a.attempt_id,
                "pre-check: overlap with running attempt; conflict + wait"
            );
            upload_conflict(s3, &a.attempt_id, push_id, claim).await?;
            wait_for_attempt_done(s3, &a.attempt_id, poll_interval).await?;
        }
    }
}

/// Post-push coordination: catches attempts that *started* during our
/// upload window. Their mark phase may not have seen our generation root
/// (we hadn't written it yet) and they may have included one of our
/// closure paths in their delete_set. Conflict + wait + re-run nix copy
/// (idempotent — only re-uploads what's actually missing now).
///
/// `horizon` advances to "now" after each successful re-run so the next
/// iteration only worries about *newer* attempts. Loops until no
/// suspect attempts remain.
async fn post_check_loop(
    s3: &S3Client,
    push_id: &str,
    claim: &HashSet<String>,
    initial_horizon: DateTime<Utc>,
    closure_root: &NixStorePath,
    copy_uri: &str,
    poll_interval: Duration,
) -> Result<()> {
    let mut horizon = initial_horizon;
    loop {
        let attempts = prune::list_running_attempts(s3).await?;
        let suspect: Vec<&PruneAttempt> = attempts
            .iter()
            .filter(|a| a.started_at >= horizon)
            .filter(|a| has_overlap(a, claim))
            .collect();
        if suspect.is_empty() {
            return Ok(());
        }
        for a in suspect {
            tracing::info!(
                attempt_id = %a.attempt_id,
                started_at = %a.started_at,
                "post-check: new attempt overlaps; conflict + wait + re-run nix copy"
            );
            upload_conflict(s3, &a.attempt_id, push_id, claim).await?;
            wait_for_attempt_done(s3, &a.attempt_id, poll_interval).await?;
            re_run_nix_copy(closure_root, copy_uri)?;
        }
        horizon = Utc::now();
    }
}

fn has_overlap(attempt: &PruneAttempt, claim: &HashSet<String>) -> bool {
    attempt.delete_set.iter().any(|h| claim.contains(h))
}

fn load_snapshot(path: &Path) -> Result<HashSet<StorePath>> {
    let content = std::fs::read(path)
        .with_context(|| format!("failed to read snapshot file: {}", path.display()))?;
    nix::parse_path_info(&content)
        .with_context(|| format!("failed to parse snapshot JSON from: {}", path.display()))
}

pub async fn run(args: PushArgs) -> Result<()> {
    // Refuse to run without our post-build hook installed. The hook is what
    // signs and uploads each build's closure as it lands; without it, this
    // binary would have to sign and push paths itself, and the in-process
    // path doesn't do that today. Fail fast rather than silently uploading
    // unsigned narinfos that clients will then reject.
    if !nix::our_post_build_hook_installed_real() {
        anyhow::bail!(
            "post-build hook {} is not the active nix daemon's post-build-hook; \
             nix-s3-generations relies on the hook to sign and upload paths. \
             Run the action's main step (which installs the hook and restarts \
             the daemon) before invoking push.",
            nix::OUR_POST_BUILD_HOOK_PATH,
        );
    }

    tracing::info!(snapshot_before = %args.snapshot_before.display(), "loading pre-step snapshot");
    let before = load_snapshot(&args.snapshot_before)?;

    tracing::info!("taking current store snapshot");
    let after = nix::snapshot_store_real().context("failed to take current store snapshot")?;

    let new_paths = nix::diff_snapshots(&before, &after);
    tracing::info!(count = new_paths.len(), "found new store paths");

    // Track all new paths (substituted + locally-built), not just ultimate
    // ones: substituted paths from upstream caches are still "in use" by
    // this run, and we want them claimed by this generation so age-based GC
    // doesn't drop them while they're being actively pulled.
    //
    // Even if `new_paths` is empty (nothing built or substituted), we still
    // build a marker closure root and write the generation file — that
    // updates the run's "last seen" timestamp so older generations age out
    // correctly relative to it.
    let cover = nix::minimal_closure_cover(&new_paths, &after);
    tracing::info!(
        new = new_paths.len(),
        cover = cover.len(),
        "computed minimal cover of new paths"
    );

    // The pusher's "claim set": every hash whose narinfo must remain on
    // S3 for the generation root to be valid. Computed locally from the
    // pre-build snapshot so any concurrent prune we conflict against
    // can subtract these hashes from its delete plan.
    let claim = nix::closure_hashes(&cover, &after);
    tracing::info!(claim_size = claim.len(), "computed pusher claim set");

    let endpoint_for_uri = args.endpoint.clone();
    let s3_client = S3Client::new(args.bucket.clone(), args.region.clone(), args.endpoint)
        .await
        .context("failed to create S3 client")?;

    let push_id = coordination::new_id();
    let copy_uri = build_copy_uri(&args.bucket, &args.region, endpoint_for_uri.as_deref());

    // ── Pre-check ──────────────────────────────────────────────────
    // Conflict-and-wait against any running attempt whose delete_set
    // overlaps our claim before letting the post-build-hook upload.
    // Otherwise nix copy would HEAD-skip paths on S3 that the prune is
    // about to delete, leaving our gen root with dangling references.
    pre_check_loop(&s3_client, &push_id, &claim, POLL_INTERVAL).await?;

    // Anchor the post-check horizon BEFORE the build/upload so any
    // attempt that starts during our window is detectable later.
    let pre_check_at = Utc::now();

    tracing::info!(
        cover = cover.len(),
        "building closure root — the post-build-hook will also push its \
         closure to the cache; expect this step to dominate runtime",
    );
    let closure_root =
        nix::build_closure_root_real(&cover).context("failed to materialize closure root")?;
    tracing::info!(%closure_root, "closure root built");

    // The post-build hook just ran a recursive `nix copy --to` on the
    // closure root. `nix copy` walks references-first (post-order), so if
    // the closure root's narinfo is in the destination, every path in its
    // closure is too. One HEAD beats re-walking 7000+ paths to confirm.
    let narinfo = s3_keys::narinfo_key(&closure_root)?;
    tracing::info!(narinfo = %narinfo, "verifying closure root narinfo on remote");
    if !s3_client
        .object_exists(&narinfo)
        .await
        .context("failed to HEAD closure root narinfo")?
    {
        anyhow::bail!(
            "closure root narinfo {} missing on remote — post-build hook \
             should have uploaded it. Treating as a cache invariant \
             violation rather than papering over it with a recursive push",
            narinfo
        );
    }
    tracing::info!("closure root narinfo verified on remote");

    // ── Post-check ─────────────────────────────────────────────────
    // Catch attempts that started during our upload window. For each
    // overlapping one, conflict + wait + re-run nix copy. nix copy is
    // idempotent — only re-uploads what HEAD now reports missing.
    post_check_loop(
        &s3_client,
        &push_id,
        &claim,
        pre_check_at,
        &closure_root,
        &copy_uri,
        POLL_INTERVAL,
    )
    .await?;

    let generation_root = GenerationRoot::new(closure_root.clone());

    let gen_root_json = serde_json::to_vec_pretty(&generation_root)
        .context("failed to serialize generation root")?;

    let gen_key = s3_keys::generations_key(&args.domain, args.generation, &args.shard_id);
    s3_client
        .upload_object(&gen_key, gen_root_json, Some("application/json"))
        .await
        .context("failed to upload generation root")?;

    tracing::info!(key = %gen_key, "Push complete. Generation root uploaded.");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn load_snapshot_parses_valid_json() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let json = r#"{
            "/nix/store/aaa": {"deriver": null, "ultimate": true, "references": []},
            "/nix/store/bbb": {"deriver": null, "ultimate": false, "references": []}
        }"#;
        tmp.write_all(json.as_bytes()).unwrap();

        let result = load_snapshot(tmp.path()).unwrap();
        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .any(|p| p.path.as_str() == "/nix/store/aaa" && p.ultimate)
        );
        assert!(
            result
                .iter()
                .any(|p| p.path.as_str() == "/nix/store/bbb" && !p.ultimate)
        );
    }

    #[test]
    fn load_snapshot_errors_on_missing_file() {
        let result = load_snapshot(Path::new("/nonexistent/path/snapshot.json"));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("failed to read snapshot file"));
    }

    #[test]
    fn load_snapshot_errors_on_invalid_json() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"not json").unwrap();

        let result = load_snapshot(tmp.path());
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("failed to parse snapshot JSON"));
    }

    #[test]
    fn load_snapshot_round_trip() {
        let json = r#"{
            "/nix/store/abc": {"deriver": null, "ultimate": true, "references": []},
            "/nix/store/def": {"deriver": null, "ultimate": false, "references": []}
        }"#;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(json.as_bytes()).unwrap();

        let loaded = load_snapshot(tmp.path()).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(
            loaded
                .iter()
                .any(|p| p.path.as_str() == "/nix/store/abc" && p.ultimate)
        );
        assert!(
            loaded
                .iter()
                .any(|p| p.path.as_str() == "/nix/store/def" && !p.ultimate)
        );
    }

    #[test]
    fn validate_shard_id_accepts_simple() {
        assert_eq!(validate_shard_id("gen").unwrap(), "gen");
        assert_eq!(
            validate_shard_id("GitHub Actions 1").unwrap(),
            "GitHub Actions 1"
        );
    }

    #[test]
    fn validate_shard_id_rejects_empty() {
        assert!(validate_shard_id("").is_err());
    }

    #[test]
    fn validate_shard_id_rejects_slash() {
        assert!(validate_shard_id("a/b").is_err());
        assert!(validate_shard_id("/leading").is_err());
        assert!(validate_shard_id("trailing/").is_err());
    }

    #[test]
    fn validate_shard_id_rejects_dot_traversal() {
        assert!(validate_shard_id(".").is_err());
        assert!(validate_shard_id("..").is_err());
    }

    // ── copy URI builder ──────────────────────────────────────────

    #[test]
    fn copy_uri_native_aws_no_endpoint() {
        assert_eq!(
            build_copy_uri("my-bucket", "us-east-1", None),
            "s3://my-bucket?region=us-east-1"
        );
    }

    #[test]
    fn copy_uri_native_aws_with_s3_endpoint() {
        assert_eq!(
            build_copy_uri("my-bucket", "us-east-1", Some("s3.amazonaws.com")),
            "s3://my-bucket?region=us-east-1"
        );
    }

    #[test]
    fn copy_uri_native_aws_regional_endpoint() {
        assert_eq!(
            build_copy_uri("my-bucket", "eu-west-2", Some("s3.eu-west-2.amazonaws.com")),
            "s3://my-bucket?region=eu-west-2"
        );
    }

    #[test]
    fn copy_uri_third_party_endpoint() {
        assert_eq!(
            build_copy_uri("my-bucket", "auto", Some("foo.r2.cloudflarestorage.com")),
            "s3://my-bucket?endpoint=foo.r2.cloudflarestorage.com&region=auto"
        );
    }

    // ── coordination flow ─────────────────────────────────────────

    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;

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

    fn empty_attempts_list_xml() -> String {
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
</ListBucketResult>"#
            .to_string()
    }

    fn list_one_attempt_xml(attempt_id: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>meta/prune-attempts/{attempt_id}.json</Key><LastModified>2026-05-09T12:00:00.000Z</LastModified></Contents>
</ListBucketResult>"#
        )
    }

    fn attempt_marker_json(attempt_id: &str, status: &str, delete_set: &[&str]) -> String {
        let ds: Vec<String> = delete_set.iter().map(|s| format!("\"{s}\"")).collect();
        let ds_arr = format!("[{}]", ds.join(","));
        let ended = if status == "done" {
            "\"2026-05-09T12:00:01Z\""
        } else {
            "null"
        };
        format!(
            r#"{{"version":1,"attempt_id":"{attempt_id}","started_at":"2026-05-09T12:00:00Z","ended_at":{ended},"status":"{status}","delete_set":{ds_arr}}}"#
        )
    }

    #[tokio::test]
    async fn pre_check_loop_no_attempts_short_circuits() {
        // Single LIST returns empty — pre_check_loop returns immediately.
        // StaticReplayClient panics on extra requests.
        let claim: HashSet<String> = ["aaaa".to_string(), "bbbb".to_string()]
            .into_iter()
            .collect();
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(empty_attempts_list_xml()))
                .unwrap(),
        )]);

        pre_check_loop(&client, "push-1", &claim, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pre_check_loop_no_overlap_does_not_conflict() {
        // One running attempt, but delete_set doesn't overlap our claim.
        // Expected events: LIST attempts → GET marker → no conflict.
        let claim: HashSet<String> = ["aaaa".to_string()].into_iter().collect();
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_one_attempt_xml("att1")))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(attempt_marker_json(
                        "att1",
                        "running",
                        &["zzzz"],
                    )))
                    .unwrap(),
            ),
        ]);

        pre_check_loop(&client, "push-1", &claim, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn pre_check_loop_overlap_conflicts_then_waits_then_proceeds() {
        // One running attempt with overlap. Expect:
        //   1. LIST attempts
        //   2. GET marker (running, has our hash)
        //   3. PUT conflict
        //   4. GET marker (still running)
        //   5. GET marker (done)
        //   6. LIST attempts (empty — second pre-check pass)
        let attempt_id = "att1";
        let claim: HashSet<String> = ["aaaa".to_string()].into_iter().collect();
        let client = mock_client(vec![
            // 1. LIST attempts
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_one_attempt_xml(attempt_id)))
                    .unwrap(),
            ),
            // 2. GET marker (running, includes our claim hash)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(attempt_marker_json(
                        attempt_id,
                        "running",
                        &["aaaa"],
                    )))
                    .unwrap(),
            ),
            // 3. PUT conflict
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // 4. GET marker (still running — wait keeps polling)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(attempt_marker_json(
                        attempt_id,
                        "running",
                        &["aaaa"],
                    )))
                    .unwrap(),
            ),
            // 5. GET marker (done — wait returns)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(attempt_marker_json(
                        attempt_id,
                        "done",
                        &["aaaa"],
                    )))
                    .unwrap(),
            ),
            // 6. LIST attempts again (empty — pre_check_loop converges)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(empty_attempts_list_xml()))
                    .unwrap(),
            ),
        ]);

        pre_check_loop(&client, "push-1", &claim, Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_attempt_done_returns_immediately_when_marker_done() {
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(attempt_marker_json("a", "done", &[])))
                .unwrap(),
        )]);
        wait_for_attempt_done(&client, "a", Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_attempt_done_treats_missing_marker_as_done() {
        // 404 on the marker key — janitor likely GC'd it. Treat as done.
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(404)
                .body(SdkBody::empty())
                .unwrap(),
        )]);
        wait_for_attempt_done(&client, "a", Duration::from_millis(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn upload_conflict_writes_expected_key() {
        let claim: HashSet<String> = ["aaaa".to_string()].into_iter().collect();
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .unwrap(),
        )]);
        upload_conflict(&client, "att1", "push-1", &claim)
            .await
            .unwrap();
    }
}
