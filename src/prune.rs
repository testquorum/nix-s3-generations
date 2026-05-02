use std::collections::{HashMap, HashSet};

use anyhow::Result;
use clap::Args;
use futures::stream::{self, StreamExt};

use crate::generation::GenerationRoot;
use crate::narinfo;
use crate::s3::S3Client;
use crate::s3_keys;

/// Maximum number of concurrent S3 GETs per mark-phase wave.
const MARK_CONCURRENCY: usize = 32;

#[derive(Args, Debug, Clone)]
pub struct PruneArgs {
    #[arg(long)]
    pub bucket: String,

    #[arg(long, default_value = "us-east-1")]
    pub region: String,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(long, default_value = "false")]
    pub dry_run: bool,
}

pub async fn run(args: PruneArgs) -> Result<()> {
    let s3 = S3Client::new(
        args.bucket.clone(),
        args.region.clone(),
        args.endpoint.clone(),
    )
    .await?;

    tracing::info!(bucket = %args.bucket, region = %args.region, dry_run = args.dry_run, "starting prune");

    let live_hashes = mark_phase(&s3).await?;

    let stats = sweep_phase(&s3, &live_hashes, args.dry_run).await?;

    tracing::info!(
        narinfos_checked = stats.narinfos_checked,
        narinfos_deleted = stats.narinfos_deleted,
        nars_deleted = stats.nars_deleted,
        dry_run = args.dry_run,
        "prune complete"
    );

    Ok(())
}

/// State of a NARinfo during mark phase traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NarinfoState {
    /// NARinfo was fetched and parsed successfully.
    Live,
    /// NARinfo returned 404 or failed to parse (visited but dead).
    Dead,
}

/// Discover the transitive closure of all live store paths reachable from
/// generational GC roots stored under `generations/`.
///
/// 1. List all objects with prefix `generations/`.
/// 2. For each `.json` key, fetch and parse as `GenerationRoot` to get GC roots.
/// 3. Convert each root's `store_path` to a narinfo S3 key.
/// 4. DFS over NARinfos: fetch, parse, convert each `References` entry to a
///    narinfo key, and enqueue new keys. Skip (with a warning) any NARinfo
///    that returns 404 or fails to parse.
/// 5. Return the set of hash strings (the part before `.narinfo`) for every
///    live path.
pub async fn mark_phase(s3: &S3Client) -> Result<HashSet<String>> {
    tracing::info!("listing generation objects under generations/");
    let keys = s3.list_objects("generations/").await?;

    let gen_keys: Vec<String> = keys.into_iter().filter(|k| k.ends_with(".json")).collect();
    tracing::info!(generation_files = gen_keys.len(), "listed generation files");

    let gc_roots: Vec<String> = stream::iter(gen_keys.into_iter())
        .map(|key| fetch_generation_root(s3, key))
        .buffer_unordered(MARK_CONCURRENCY)
        .filter_map(|r| async move { r })
        .collect()
        .await;

    tracing::info!(
        gc_roots = gc_roots.len(),
        "collected GC roots from generation files"
    );

    let mut narinfo_states: HashMap<String, NarinfoState> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();

    for root in &gc_roots {
        let nsp = match crate::nix::NixStorePath::try_from(root.clone()) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(store_path = %root, error = %e, "invalid store path in generation root, skipping");
                continue;
            }
        };
        let narinfo_key = match s3_keys::narinfo_key(&nsp) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(store_path = %nsp, error = %e, "failed to derive narinfo key from GC root, skipping");
                continue;
            }
        };
        if !narinfo_states.contains_key(&narinfo_key) {
            narinfo_states.insert(narinfo_key.clone(), NarinfoState::Dead);
            stack.push(narinfo_key);
        }
    }

    let mut dfs_count = 0u64;

    while !stack.is_empty() {
        let wave_size = stack.len();
        dfs_count += wave_size as u64;

        let wave: Vec<(String, NarinfoOutcome)> = stream::iter(stack.drain(..))
            .map(|key| fetch_and_parse_narinfo(s3, key))
            .buffer_unordered(MARK_CONCURRENCY)
            .collect()
            .await;

        for (key, outcome) in wave {
            match outcome {
                NarinfoOutcome::Live(refs) => {
                    narinfo_states.insert(key, NarinfoState::Live);
                    for ref_key in refs {
                        if !narinfo_states.contains_key(&ref_key) {
                            narinfo_states.insert(ref_key.clone(), NarinfoState::Dead);
                            stack.push(ref_key);
                        }
                    }
                }
                NarinfoOutcome::Dead => {
                    narinfo_states.insert(key, NarinfoState::Dead);
                }
            }
        }

        let live_count = narinfo_states
            .values()
            .filter(|s| **s == NarinfoState::Live)
            .count();
        tracing::info!(
            processed = dfs_count,
            live = live_count,
            wave = wave_size,
            "mark phase wave complete"
        );
    }

    let live_count = narinfo_states
        .values()
        .filter(|s| **s == NarinfoState::Live)
        .count();

    tracing::info!(
        "mark phase complete — DFS traversed {} narinfos, found {} live paths",
        dfs_count,
        live_count,
    );

    let live_hashes: HashSet<String> = narinfo_states
        .into_iter()
        .filter(|(_, state)| *state == NarinfoState::Live)
        .map(|(key, _)| key)
        .map(|key| key.strip_suffix(".narinfo").unwrap_or(&key).to_string())
        .collect();

    Ok(live_hashes)
}

/// Outcome of fetching + parsing a single narinfo during the mark phase.
enum NarinfoOutcome {
    /// Narinfo was fetched and parsed; the inner `Vec` holds newly-discovered
    /// narinfo S3 keys (references already converted via `hash_name_to_narinfo_key`).
    Live(Vec<String>),
    /// Narinfo returned 404 or failed to parse — visited but dead.
    Dead,
}

/// Fetch a single generation JSON and extract its store path. Returns `None`
/// (with a logged warning) on fetch / parse failure.
async fn fetch_generation_root(s3: &S3Client, key: String) -> Option<String> {
    let body = match s3.get_object(&key).await {
        Ok(data) => data,
        Err(e) => {
            tracing::warn!(key = %key, error = %e, "failed to fetch generation JSON, skipping");
            return None;
        }
    };

    let root: GenerationRoot = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(key = %key, error = %e, "failed to parse generation JSON, skipping");
            return None;
        }
    };

    Some(root.store_path.as_str().to_string())
}

/// Fetch a single narinfo, parse it, and return the discovered references as
/// narinfo S3 keys. Per-node work is pure outside the S3 GET — safe to call
/// concurrently from a wave.
async fn fetch_and_parse_narinfo(s3: &S3Client, narinfo_key: String) -> (String, NarinfoOutcome) {
    let body = match s3.get_object(&narinfo_key).await {
        Ok(data) => data,
        Err(e) => {
            if is_not_found(&e) {
                tracing::warn!(key = %narinfo_key, "NARinfo not found (404), skipping references");
            } else {
                tracing::warn!(key = %narinfo_key, error = %e, "failed to fetch NARinfo, skipping references");
            }
            return (narinfo_key, NarinfoOutcome::Dead);
        }
    };

    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(key = %narinfo_key, error = %e, "NARinfo body is not valid UTF-8, skipping");
            return (narinfo_key, NarinfoOutcome::Dead);
        }
    };

    match narinfo::parse_narinfo(body_str) {
        Ok(info) => {
            let refs: Vec<String> = info
                .references
                .iter()
                .map(|r| s3_keys::hash_name_to_narinfo_key(r))
                .collect();
            (narinfo_key, NarinfoOutcome::Live(refs))
        }
        Err(e) => {
            tracing::warn!(key = %narinfo_key, error = %e, "failed to parse NARinfo, skipping references");
            (narinfo_key, NarinfoOutcome::Dead)
        }
    }
}

/// Statistics produced by a sweep phase run.
#[derive(Debug, Default)]
pub struct PruneStats {
    pub narinfos_checked: usize,
    pub narinfos_deleted: usize,
    pub nars_deleted: usize,
}

/// Delete unreachable narinfo + NAR pairs.
///
/// 1. List ALL objects in the bucket.
/// 2. Filter to narinfo keys via [`s3_keys::is_narinfo_key`].
/// 3. For each narinfo whose hash (key without `.narinfo`) is **not** in
///    `live_hashes`:
///    - **Guardrails**: skip keys starting with `generations/` or equal to
///      `nix-cache-info`.
///    - `dry_run`: log and increment counters without deleting.
///    - otherwise: fetch the narinfo body, parse the `URL` field, validate
///      with [`s3_keys::sanitize_nar_url`], then delete both the narinfo
///      key and the NAR file.
/// 4. If the narinfo body cannot be fetched or parsed the narinfo key is
///    still deleted (we know it is dead).
pub async fn sweep_phase(
    s3: &S3Client,
    live_hashes: &HashSet<String>,
    dry_run: bool,
) -> Result<PruneStats> {
    tracing::info!("listing all objects in bucket for sweep phase");
    let keys = s3.list_objects("").await?;

    let narinfo_keys: Vec<&String> = keys.iter().filter(|k| s3_keys::is_narinfo_key(k)).collect();

    let mut stats = PruneStats::default();
    let mut sweep_count = 0usize;

    for key in &narinfo_keys {
        sweep_count += 1;
        stats.narinfos_checked += 1;

        if sweep_count.is_multiple_of(100) {
            tracing::info!(
                "sweep: processed {} narinfos, {} deleted",
                sweep_count,
                stats.narinfos_deleted
            );
        }

        let hash = key.strip_suffix(".narinfo").unwrap_or(key);

        if live_hashes.contains(hash) {
            continue;
        }

        // --- Guardrails ---
        if key.starts_with("generations/") {
            tracing::warn!(
                key = key.as_str(),
                "skipping generations/ narinfo (guardrail)"
            );
            continue;
        }
        if key.as_str() == "nix-cache-info" {
            tracing::warn!(
                key = "nix-cache-info",
                "skipping nix-cache-info (guardrail)"
            );
            continue;
        }

        if dry_run {
            tracing::info!(key = key.as_str(), "[DRY RUN] would delete narinfo + NAR");
            stats.narinfos_deleted += 1;
            stats.nars_deleted += 1;
            continue;
        }

        // --- Delete the narinfo key (always, even if body fetch fails) ---
        let nar_key_opt: Option<String> = match s3.get_object(key).await {
            Ok(data) => match std::str::from_utf8(&data) {
                Ok(body_str) => match narinfo::parse_narinfo(body_str) {
                    Ok(info) => match s3_keys::sanitize_nar_url(&info.url) {
                        Ok(url) => Some(url),
                        Err(e) => {
                            tracing::warn!(key = key.as_str(), url = %info.url, error = %e, "dead narinfo has invalid URL, deleting key anyway");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!(key = key.as_str(), error = %e, "failed to parse dead narinfo, deleting key anyway");
                        None
                    }
                },
                Err(e) => {
                    tracing::warn!(key = key.as_str(), error = %e, "dead narinfo body is not valid UTF-8, deleting key anyway");
                    None
                }
            },
            Err(e) => {
                tracing::warn!(key = key.as_str(), error = %e, "failed to fetch dead narinfo, deleting key anyway");
                None
            }
        };

        tracing::info!(key = key.as_str(), "deleting dead narinfo");
        match s3.delete_object(key).await {
            Ok(_) => {
                stats.narinfos_deleted += 1;
            }
            Err(e) => {
                tracing::error!(key = key.as_str(), error = %e, "failed to delete narinfo key");
            }
        }

        if let Some(nar_key) = nar_key_opt {
            tracing::info!(key = %nar_key, "deleting dead NAR");
            match s3.delete_object(&nar_key).await {
                Ok(_) => {
                    stats.nars_deleted += 1;
                }
                Err(e) => {
                    tracing::error!(key = %nar_key, error = %e, "failed to delete NAR file");
                }
            }
        }
    }

    tracing::info!(
        narinfos_checked = stats.narinfos_checked,
        narinfos_deleted = stats.narinfos_deleted,
        nars_deleted = stats.nars_deleted,
        dry_run = dry_run,
        "sweep phase complete",
    );

    Ok(stats)
}

fn is_not_found(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(aws_sdk_s3::error::SdkError::ServiceError(inner)) = cause
            .downcast_ref::<aws_sdk_s3::error::SdkError<
            aws_sdk_s3::operation::get_object::GetObjectError,
        >>() {
            return inner.raw().status().as_u16() == 404;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn list_xml(keys: &[&str]) -> String {
        let contents: String = keys
            .iter()
            .map(|k| format!("  <Contents><Key>{k}</Key></Contents>\n"))
            .collect();
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
{contents}</ListBucketResult>"#
        )
    }

    fn generation_list_xml(keys: &[&str]) -> String {
        list_xml(keys)
    }

    fn narinfo_body(hash: &str, references: &[&str]) -> String {
        let refs = references.join(" ");
        format!(
            "StorePath: /nix/store/{hash}-pkg\n\
             URL: nar/{hash}.nar.xz\n\
             References: {refs}\n"
        )
    }

    fn generation_json(store_path: &str) -> String {
        format!(
            r#"{{
  "version": 1,
  "timestamp": "2026-01-01T00:00:00Z",
  "storePath": "{store_path}"
}}"#
        )
    }

    #[tokio::test]
    async fn transitive_references() {
        let client = mock_client(vec![
            // list_objects("generations/")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/gen.json",
                    ])))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/gen.json")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
            // get_object("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo") — A
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        &["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-pkg-b"],
                    )))
                    .unwrap(),
            ),
            // get_object("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.narinfo") — B
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        &["cccccccccccccccccccccccccccccccc-pkg-c"],
                    )))
                    .unwrap(),
            ),
            // get_object("cccccccccccccccccccccccccccccccc.narinfo") — C (no refs)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "cccccccccccccccccccccccccccccccc",
                        &[],
                    )))
                    .unwrap(),
            ),
        ]);

        let live = mark_phase(&client).await.unwrap();
        assert_eq!(live.len(), 3);
        assert!(live.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(live.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        assert!(live.contains("cccccccccccccccccccccccccccccccc"));
    }

    #[tokio::test]
    async fn missing_narinfo_is_skipped() {
        // A references B, but B is 404. Mark should still include A.
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/gen.json",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
            // A narinfo
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        &["bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-pkg-b"],
                    )))
                    .unwrap(),
            ),
            // B narinfo — 404
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(404)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let live = mark_phase(&client).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(live.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    #[tokio::test]
    async fn self_reference_does_not_infinite_loop() {
        // A references A (self-reference). Should still terminate.
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/gen.json",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        &["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a"],
                    )))
                    .unwrap(),
            ),
        ]);

        let live = mark_phase(&client).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(live.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    #[tokio::test]
    async fn corrupt_generation_json_is_skipped() {
        // Two generation files: one valid, one corrupt. The corrupt one should
        // be skipped with a warning; the valid one still produces its root.
        let client = mock_client(vec![
            // list_objects
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/good.json",
                        "generations/dom/1/bad.json",
                    ])))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/good.json")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/bad.json") — returns corrupt data
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from("this is not valid json {{{"))
                    .unwrap(),
            ),
            // narinfo for A (no refs)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        &[],
                    )))
                    .unwrap(),
            ),
        ]);

        let live = mark_phase(&client).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(live.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    #[tokio::test]
    async fn empty_generations_prefix() {
        // No generation files at all — should return empty set.
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(generation_list_xml(&[])))
                .unwrap(),
        )]);

        let live = mark_phase(&client).await.unwrap();
        assert!(live.is_empty());
    }

    #[tokio::test]
    async fn skips_non_json_keys_in_generations() {
        // generations/ contains a .json file plus unrelated files; only the
        // .json should be considered.
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/README.txt",
                        "generations/dom/1/gen.json",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        &[],
                    )))
                    .unwrap(),
            ),
        ]);

        let live = mark_phase(&client).await.unwrap();
        assert_eq!(live.len(), 1);
        assert!(live.contains("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    }

    // ── sweep_phase tests ──────────────────────────────────────────

    #[tokio::test]
    async fn sweep_deletes_dead_narinfos_and_nars() {
        let mut live: HashSet<String> = HashSet::new();
        live.insert("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string());

        let client = mock_client(vec![
            // list_objects("") — full bucket listing
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                        "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                        "nar/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.nar.xz",
                        "nar/deaddeaddeaddeaddeaddeaddeaddead.nar.xz",
                        "generations/dom/1/gen.json",
                    ])))
                    .unwrap(),
            ),
            // get_object("deaddeaddeaddeaddeaddeaddeaddead.narinfo") — fetch body to get URL
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "deaddeaddeaddeaddeaddeaddeaddead",
                        &[],
                    )))
                    .unwrap(),
            ),
            // delete_object("deaddeaddeaddeaddeaddeaddeaddead.narinfo")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // delete_object("nar/deaddeaddeaddeaddeaddeaddeaddead.nar.xz")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_phase(&client, &live, false).await.unwrap();
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
        assert_eq!(stats.nars_deleted, 1);
    }

    #[tokio::test]
    async fn sweep_dry_run_counts_but_does_not_delete() {
        let mut live: HashSet<String> = HashSet::new();
        live.insert("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string());

        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(list_xml(&[
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                    "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                ])))
                .unwrap(),
        )]);

        let stats = sweep_phase(&client, &live, true).await.unwrap();
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
        assert_eq!(stats.nars_deleted, 1);
        // No delete_object calls were made — StaticReplayClient would
        // panic if unexpected requests were sent.
    }

    #[tokio::test]
    async fn sweep_skips_guardrail_prefixes() {
        let live: HashSet<String> = HashSet::new();

        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                        "generations/x.narinfo",
                    ])))
                    .unwrap(),
            ),
            // Only the non-guardrail dead narinfo is fetched + deleted
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(
                        "deaddeaddeaddeaddeaddeaddeaddead",
                        &[],
                    )))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_phase(&client, &live, false).await.unwrap();
        // Both narinfos are counted as checked, but generations/ one is skipped
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
    }

    // ── integration test ───────────────────────────────────────────

    #[tokio::test]
    async fn test_prune_full_cycle() {
        // 3 generation roots, 10 NARinfos (8 live, 2 dead).
        // Live graph:
        //   A -> B, C    D -> E    F -> G -> H
        //   C -> B (already visited)    B, E, H: leaf nodes
        // Dead: I, J (not reachable from any root)
        let a_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let b_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let c_hash = "cccccccccccccccccccccccccccccccc";
        let d_hash = "dddddddddddddddddddddddddddddddd";
        let e_hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let f_hash = "ffffffffffffffffffffffffffffffff";
        let g_hash = "gggggggggggggggggggggggggggggggg";
        let h_hash = "hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh";
        let i_hash = "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii";
        let j_hash = "jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj";

        let a_refs = &[
            &format!("{b_hash}-pkg-b") as &str,
            &format!("{c_hash}-pkg-c") as &str,
        ];
        let c_refs = &[&format!("{b_hash}-pkg-b") as &str];
        let d_refs = &[&format!("{e_hash}-pkg-e") as &str];
        let f_refs = &[&format!("{g_hash}-pkg-g") as &str];
        let g_refs = &[&format!("{h_hash}-pkg-h") as &str];
        let no_refs: &[&str] = &[];

        let client = mock_client(vec![
            // ── mark_phase ──
            // list_objects("generations/")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_list_xml(&[
                        "generations/dom/1/gen-a.json",
                        "generations/dom/1/gen-d.json",
                        "generations/dom/1/gen-f.json",
                    ])))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/gen-a.json")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{a_hash}-pkg-a"
                    ))))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/gen-d.json")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{d_hash}-pkg-d"
                    ))))
                    .unwrap(),
            ),
            // get_object("generations/dom/1/gen-f.json")
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{f_hash}-pkg-f"
                    ))))
                    .unwrap(),
            ),
            // BFS: get A.narinfo → refs [B, C]
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(a_hash, a_refs)))
                    .unwrap(),
            ),
            // get D.narinfo → refs [E]
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(d_hash, d_refs)))
                    .unwrap(),
            ),
            // get F.narinfo → refs [G]
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(f_hash, f_refs)))
                    .unwrap(),
            ),
            // get B.narinfo → no refs
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(b_hash, no_refs)))
                    .unwrap(),
            ),
            // get C.narinfo → refs [B] (already visited)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(c_hash, c_refs)))
                    .unwrap(),
            ),
            // get E.narinfo → no refs
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(e_hash, no_refs)))
                    .unwrap(),
            ),
            // get G.narinfo → refs [H]
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(g_hash, g_refs)))
                    .unwrap(),
            ),
            // get H.narinfo → no refs
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(h_hash, no_refs)))
                    .unwrap(),
            ),
            // ── sweep_phase ──
            // list_objects("") — 10 narinfo keys + extras
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        &format!("{a_hash}.narinfo"),
                        &format!("{b_hash}.narinfo"),
                        &format!("{c_hash}.narinfo"),
                        &format!("{d_hash}.narinfo"),
                        &format!("{e_hash}.narinfo"),
                        &format!("{f_hash}.narinfo"),
                        &format!("{g_hash}.narinfo"),
                        &format!("{h_hash}.narinfo"),
                        &format!("{i_hash}.narinfo"),
                        &format!("{j_hash}.narinfo"),
                        &format!("nar/{a_hash}.nar.xz"),
                        &format!("nar/{d_hash}.nar.xz"),
                        &format!("nar/{i_hash}.nar.xz"),
                        &format!("nar/{j_hash}.nar.xz"),
                        "generations/dom/1/gen-a.json",
                    ])))
                    .unwrap(),
            ),
            // I.narinfo (dead) → fetch + delete
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(i_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // J.narinfo (dead) → fetch + delete
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(j_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        // Act
        let live = mark_phase(&client).await.unwrap();
        let stats = sweep_phase(&client, &live, false).await.unwrap();

        // Assert mark_phase: 8 live hashes
        assert_eq!(live.len(), 8);
        assert!(live.contains(a_hash));
        assert!(live.contains(b_hash));
        assert!(live.contains(c_hash));
        assert!(live.contains(d_hash));
        assert!(live.contains(e_hash));
        assert!(live.contains(f_hash));
        assert!(live.contains(g_hash));
        assert!(live.contains(h_hash));
        assert!(!live.contains(i_hash));
        assert!(!live.contains(j_hash));

        // Assert sweep_phase stats
        assert_eq!(stats.narinfos_checked, 10);
        assert_eq!(stats.narinfos_deleted, 2);
        assert_eq!(stats.nars_deleted, 2);
    }
}
