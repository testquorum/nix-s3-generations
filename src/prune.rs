use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand};
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::generation::GenerationRoot;
use crate::narinfo;
use crate::s3::S3Client;
use crate::s3_keys;

/// Maximum number of concurrent S3 GETs per mark-phase wave.
pub const MARK_CONCURRENCY: usize = 32;

/// Default freshness buffer (seconds): objects whose `LastModified` is within
/// this window of prune start are protected from sweep. Doubles as
/// clock-skew margin between the runner and S3, and as a soft grace period
/// for in-flight pushes whose generation root JSON hasn't landed yet. 5 min
/// is comfortably larger than typical push durations and any plausible NTP
/// drift on GHA runners.
pub const DEFAULT_FRESHNESS_BUFFER_SECS: u64 = 300;

#[derive(Args, Debug, Clone)]
pub struct PruneArgs {
    /// Restrict prune to one half of the work. With no subcommand, both
    /// halves run in series: first `narinfo` (delete dead narinfos), then
    /// `nar` (delete NARs no surviving narinfo references).
    #[command(subcommand)]
    pub command: Option<PruneSubcommand>,

    #[arg(long)]
    pub bucket: String,

    #[arg(long, default_value = "us-east-1")]
    pub region: String,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(long, default_value = "false")]
    pub dry_run: bool,

    /// Skip deletion of any object whose `LastModified` is within this many
    /// seconds of prune start. Protects in-flight pushes from being raced —
    /// a push that uploaded NARs but hasn't yet written its generation root
    /// JSON is invisible to the mark phase, and without this filter sweep
    /// would delete the orphaned NARs. Also covers clock skew between the
    /// runner and S3.
    #[arg(long, default_value_t = DEFAULT_FRESHNESS_BUFFER_SECS)]
    pub freshness_buffer_secs: u64,
}

#[derive(Subcommand, Debug, Clone)]
pub enum PruneSubcommand {
    /// Walk the closure from generation roots and delete narinfos that are
    /// no longer reachable. Does not touch any NARs.
    Narinfo,
    /// Collect every NAR URL referenced by every narinfo currently in the
    /// bucket and delete NARs not in that set. Does not touch any narinfos.
    Nar,
}

pub async fn run(args: PruneArgs) -> Result<()> {
    // Combined dry-run is meaningless: phase 1 wouldn't actually delete
    // narinfos, so phase 2 would still see them in its URL scan and
    // protect every NAR. The reported orphan-NAR count would always be
    // zero. Force the user to dry-run a single half at a time.
    if args.dry_run && args.command.is_none() {
        anyhow::bail!(
            "--dry-run requires a subcommand (`narinfo` or `nar`): \
             a combined dry-run can't carry the simulated narinfo \
             deletions into the NAR phase, so its NAR sweep stats \
             would be wrong"
        );
    }

    let s3 = S3Client::new(
        args.bucket.clone(),
        args.region.clone(),
        args.endpoint.clone(),
    )
    .await?;

    // Anchor the cutoff *before* either phase runs. Any object whose
    // LastModified is at-or-after this instant is treated as "in flight"
    // and skipped, even if it isn't in the relevant live / referenced set.
    let prune_start_at = Utc::now() - Duration::seconds(args.freshness_buffer_secs as i64);

    tracing::info!(
        bucket = %args.bucket,
        region = %args.region,
        dry_run = args.dry_run,
        freshness_buffer_secs = args.freshness_buffer_secs,
        prune_start_at = %prune_start_at,
        command = ?args.command,
        "starting prune",
    );

    match args.command {
        None => {
            // Default: both halves, narinfo first so phase 2's URL scan sees
            // only the surviving narinfos.
            let live_hashes = mark_phase(&s3).await?;
            let narinfo_stats =
                sweep_narinfos(&s3, &live_hashes, prune_start_at, args.dry_run).await?;
            let nar_stats = sweep_nars(&s3, prune_start_at, args.dry_run).await?;
            log_narinfo_sweep_stats(&narinfo_stats, args.dry_run);
            log_nar_sweep_stats(&nar_stats, args.dry_run);
            tracing::info!(
                total_bytes_deleted = narinfo_stats.bytes_deleted + nar_stats.bytes_deleted,
                "prune totals",
            );
        }
        Some(PruneSubcommand::Narinfo) => {
            let live_hashes = mark_phase(&s3).await?;
            let stats = sweep_narinfos(&s3, &live_hashes, prune_start_at, args.dry_run).await?;
            log_narinfo_sweep_stats(&stats, args.dry_run);
        }
        Some(PruneSubcommand::Nar) => {
            let stats = sweep_nars(&s3, prune_start_at, args.dry_run).await?;
            log_nar_sweep_stats(&stats, args.dry_run);
        }
    }

    tracing::info!("prune complete");
    Ok(())
}

fn log_narinfo_sweep_stats(stats: &NarinfoSweepStats, dry_run: bool) {
    tracing::info!(
        narinfos_checked = stats.narinfos_checked,
        narinfos_deleted = stats.narinfos_deleted,
        bytes_deleted = stats.bytes_deleted,
        dry_run,
        "narinfo sweep complete",
    );
}

fn log_nar_sweep_stats(stats: &NarSweepStats, dry_run: bool) {
    tracing::info!(
        narinfos_scanned = stats.narinfos_scanned,
        referenced_nars = stats.referenced_nars,
        nars_checked = stats.nars_checked,
        nars_deleted = stats.nars_deleted,
        nars_kept_referenced = stats.nars_kept_referenced,
        bytes_deleted = stats.bytes_deleted,
        dry_run,
        "NAR sweep complete",
    );
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

    let mut generation_files = 0usize;
    let gen_keys = s3
        .list_objects("generations/")
        .map_ok(|o| o.key)
        .try_filter(|k| futures::future::ready(k.ends_with(".json")))
        .inspect_ok(|_| generation_files += 1);

    let gc_roots: Vec<String> = gen_keys
        .map_ok(|key| fetch_generation_root(s3, key))
        .try_buffer_unordered(MARK_CONCURRENCY)
        .try_filter_map(|opt| async move { Ok(opt) })
        .try_collect()
        .await?;

    tracing::info!(generation_files, "listed generation files");

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

        let wave_results: Vec<Result<(String, NarinfoOutcome)>> = stream::iter(stack.drain(..))
            .map(|key| fetch_and_parse_narinfo(s3, key))
            .buffer_unordered(MARK_CONCURRENCY)
            .collect()
            .await;

        for result in wave_results {
            let (key, outcome) = result?;
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

/// Fetch a single generation JSON and extract its store path.
///
/// Returns `Ok(Some(...))` on success, `Ok(None)` if the object is genuinely
/// gone (404) or the body is unparseable JSON (genuine corruption — skipped
/// with a warning). Any other fetch error (network, 5xx, retry exhaustion)
/// returns `Err`, which the caller propagates to abort the entire prune —
/// silently dropping a generation root would cause its whole closure to be
/// deleted in the sweep phase.
async fn fetch_generation_root(s3: &S3Client, key: String) -> Result<Option<String>> {
    let body = match s3.get_object(&key).await {
        Ok(data) => data,
        Err(e) => {
            if is_not_found(&e) {
                tracing::warn!(key = %key, "generation JSON not found (404), skipping");
                return Ok(None);
            }
            return Err(e.context(format!("failed to fetch generation JSON {key}")));
        }
    };

    let root: GenerationRoot = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(key = %key, error = %e, "failed to parse generation JSON, skipping");
            return Ok(None);
        }
    };

    Ok(Some(root.store_path.as_str().to_string()))
}

/// Fetch a single narinfo, parse it, and return the discovered references as
/// narinfo S3 keys. Per-node work is pure outside the S3 GET — safe to call
/// concurrently from a wave.
///
/// `Ok(Dead)` only on a genuine 404 or unparseable body. **Any other fetch
/// error returns `Err`** so the mark phase can abort: classifying a
/// transient network failure as `Dead` would cause `sweep_phase` to delete
/// a live narinfo and its NAR.
async fn fetch_and_parse_narinfo(
    s3: &S3Client,
    narinfo_key: String,
) -> Result<(String, NarinfoOutcome)> {
    let body = match s3.get_object(&narinfo_key).await {
        Ok(data) => data,
        Err(e) => {
            if is_not_found(&e) {
                tracing::warn!(key = %narinfo_key, "NARinfo not found (404), skipping references");
                return Ok((narinfo_key, NarinfoOutcome::Dead));
            }
            return Err(e.context(format!("failed to fetch NARinfo {narinfo_key}")));
        }
    };

    let body_str = match std::str::from_utf8(&body) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(key = %narinfo_key, error = %e, "NARinfo body is not valid UTF-8, skipping");
            return Ok((narinfo_key, NarinfoOutcome::Dead));
        }
    };

    match narinfo::parse_narinfo(body_str) {
        Ok(info) => {
            let refs: Vec<String> = info
                .references
                .iter()
                .map(|r| s3_keys::hash_name_to_narinfo_key(r))
                .collect();
            Ok((narinfo_key, NarinfoOutcome::Live(refs)))
        }
        Err(e) => {
            tracing::warn!(key = %narinfo_key, error = %e, "failed to parse NARinfo, skipping references");
            Ok((narinfo_key, NarinfoOutcome::Dead))
        }
    }
}

/// Statistics produced by `sweep_narinfos`.
///
/// `bytes_deleted` sums the `Size` field reported by S3 for every narinfo
/// counted in `narinfos_deleted` — including dry-run "would delete" hits.
#[derive(Debug, Default)]
pub struct NarinfoSweepStats {
    pub narinfos_checked: usize,
    pub narinfos_deleted: usize,
    pub bytes_deleted: u64,
}

/// Statistics produced by `sweep_nars`.
///
/// `bytes_deleted` sums the `Size` field reported by S3 for every NAR
/// counted in `nars_deleted` — including dry-run "would delete" hits.
#[derive(Debug, Default)]
pub struct NarSweepStats {
    pub narinfos_scanned: usize,
    pub referenced_nars: usize,
    pub nars_checked: usize,
    pub nars_deleted: usize,
    pub nars_kept_referenced: usize,
    pub bytes_deleted: u64,
}

/// Delete narinfos whose hash isn't in `live_hashes`. Does **not** touch
/// NARs — that's `sweep_nars`.
///
/// 1. List ALL objects in the bucket.
/// 2. Filter to narinfo keys whose `LastModified` predates
///    `prune_start_at` — anything more recent (or missing a
///    `LastModified` entirely) might belong to an in-flight push whose
///    generation root hasn't landed yet, so we exclude it from the
///    candidate set up-front.
/// 3. For each remaining narinfo whose hash (key without `.narinfo`) is
///    **not** in `live_hashes`:
///    - **Guardrails**: skip keys starting with `generations/` or equal to
///      `nix-cache-info`.
///    - `dry_run`: log and increment the counter without deleting.
///    - otherwise: delete the narinfo.
///
/// `sweep_nars` (run after this) re-derives the referenced NAR set from
/// the narinfos that *survive* this phase, so a deduplicated NAR shared
/// between live and dead narinfos remains protected via the live
/// narinfo's URL.
pub async fn sweep_narinfos(
    s3: &S3Client,
    live_hashes: &HashSet<String>,
    prune_start_at: DateTime<Utc>,
    dry_run: bool,
) -> Result<NarinfoSweepStats> {
    tracing::info!("listing all objects in bucket for narinfo sweep");
    let objects = s3.list_objects("");

    let mut narinfo_objects = objects
        .try_filter(|o| futures::future::ready(s3_keys::is_narinfo_key(&o.key)))
        .try_filter(|o| {
            futures::future::ready(match o.last_modified {
                Some(lm) if lm < prune_start_at => true,
                Some(lm) => {
                    tracing::info!(
                        key = o.key.as_str(),
                        last_modified = %lm,
                        cutoff = %prune_start_at,
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
        })
        .boxed();

    let mut stats = NarinfoSweepStats::default();
    let mut sweep_count = 0usize;

    while let Some(obj) = narinfo_objects.try_next().await? {
        let key = &obj.key;
        sweep_count += 1;
        stats.narinfos_checked += 1;

        if sweep_count.is_multiple_of(100) {
            tracing::info!(
                "narinfo sweep: processed {} narinfos, {} deleted",
                sweep_count,
                stats.narinfos_deleted
            );
        }

        let hash = key.strip_suffix(".narinfo").unwrap_or(key);

        if live_hashes.contains(hash) {
            continue;
        }

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

        let bytes = obj.size.unwrap_or(0) as u64;

        if dry_run {
            tracing::info!(key = key.as_str(), bytes, "[DRY RUN] would delete narinfo");
            stats.narinfos_deleted += 1;
            stats.bytes_deleted += bytes;
            continue;
        }

        tracing::info!(key = key.as_str(), bytes, "deleting dead narinfo");
        match s3.delete_object(key).await {
            Ok(_) => {
                stats.narinfos_deleted += 1;
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

    Ok(stats)
}

/// Delete NARs that no narinfo in the bucket references.
///
/// Independent of the mark phase: walks every `.narinfo` *currently in
/// S3* and collects each one's `URL:` into `referenced_nar_keys`. Then
/// walks `nar/` keys and deletes any that match the `is_nar_key` shape,
/// pass the freshness filter, and aren't in the referenced set.
///
/// Walking the live S3 state (not the mark-phase output) is what makes
/// content-deduplicated NARs safe: two distinct narinfos pointing at the
/// same `nar/<file-hash>.nar.<ext>` both contribute the URL, so the NAR
/// is preserved as long as either narinfo survives.
///
/// Any failure that prevents reading a narinfo's contents aborts the
/// whole phase: non-404 GET, non-UTF-8 body, parse error. Most of
/// these in practice mean S3 is flaking and we can't reason about the
/// rest of the bucket; the rare "genuinely corrupt narinfo" case has
/// the same shape and the same right answer — fail loud, leave the
/// bucket untouched, and let the operator decide. Manual remediation
/// for a corrupt narinfo is to delete it by hand (accepting that some
/// of its NARs may then be reaped as orphan on the next run) and
/// re-run prune.
///
/// The `URL:` field is inserted into the referenced set verbatim, no
/// validation. A weird URL (absolute, path traversal, missing
/// `nar/` prefix) can never match any key produced by the `nar/`
/// listing — those are filtered through `is_nar_key` — so it's
/// equivalent to either skipping it or inserting it. Inserting is
/// simpler.
pub async fn sweep_nars(
    s3: &S3Client,
    prune_start_at: DateTime<Utc>,
    dry_run: bool,
) -> Result<NarSweepStats> {
    tracing::info!("scanning narinfos to build referenced NAR set");

    let mut narinfos_scanned = 0usize;

    let referenced_nar_keys: HashSet<String> = s3
        .list_objects("")
        .map_ok(|o| o.key)
        .try_filter(|k| futures::future::ready(s3_keys::is_narinfo_key(k)))
        .try_filter(|k| {
            // Defensive: skip stray narinfo-suffixed keys under reserved prefixes.
            futures::future::ready(!k.starts_with("generations/") && k != "nix-cache-info")
        })
        .map_ok(|key| async move {
            let body = match s3.get_object(&key).await {
                Ok(b) => b,
                Err(e) if is_not_found(&e) => {
                    // Narinfo was deleted between listing and GET — fine, it
                    // can't be protecting any NAR if it isn't there.
                    return Ok(None);
                }
                Err(e) => {
                    return Err(
                        e.context(format!("failed to fetch narinfo {key} during NAR sweep"))
                    );
                }
            };

            let body_str = std::str::from_utf8(&body).with_context(|| {
                format!(
                    "narinfo {key} body is not valid UTF-8 during NAR sweep; \
                     aborting rather than risk deleting NARs we can't reason about. \
                     If this narinfo is genuinely corrupt, delete it manually and \
                     re-run prune."
                )
            })?;

            let info = narinfo::parse_narinfo(body_str).with_context(|| {
                format!(
                    "narinfo {key} failed to parse during NAR sweep; aborting \
                     rather than risk deleting NARs we can't reason about. If \
                     this narinfo is genuinely corrupt, delete it manually and \
                     re-run prune."
                )
            })?;

            Ok(Some(info.url))
        })
        .try_buffer_unordered(MARK_CONCURRENCY)
        .inspect_ok(|opt| {
            if opt.is_some() {
                narinfos_scanned += 1;
            }
        })
        .try_filter_map(|opt| async move { Ok(opt) })
        .try_collect()
        .await?;

    tracing::info!(
        narinfos_scanned,
        referenced_nars = referenced_nar_keys.len(),
        "narinfo scan complete; sweeping NARs",
    );

    let mut nar_objects = s3
        .list_objects("nar/")
        .try_filter(|o| futures::future::ready(s3_keys::is_nar_key(&o.key)))
        .try_filter(|o| {
            futures::future::ready(match o.last_modified {
                Some(lm) if lm < prune_start_at => true,
                Some(lm) => {
                    tracing::info!(
                        key = o.key.as_str(),
                        last_modified = %lm,
                        cutoff = %prune_start_at,
                        "skipping recently-modified NAR (freshness filter)"
                    );
                    false
                }
                None => {
                    tracing::warn!(
                        key = o.key.as_str(),
                        "NAR has no LastModified in listing, skipping defensively"
                    );
                    false
                }
            })
        })
        .boxed();

    let mut stats = NarSweepStats {
        narinfos_scanned,
        referenced_nars: referenced_nar_keys.len(),
        ..Default::default()
    };
    let mut sweep_count = 0usize;

    while let Some(obj) = nar_objects.try_next().await? {
        let key = &obj.key;
        sweep_count += 1;
        stats.nars_checked += 1;

        if sweep_count.is_multiple_of(100) {
            tracing::info!(
                "NAR sweep: processed {} NARs, {} deleted",
                sweep_count,
                stats.nars_deleted
            );
        }

        if referenced_nar_keys.contains(key.as_str()) {
            stats.nars_kept_referenced += 1;
            continue;
        }

        let bytes = obj.size.unwrap_or(0) as u64;

        if dry_run {
            tracing::info!(
                key = key.as_str(),
                bytes,
                "[DRY RUN] would delete orphan NAR"
            );
            stats.nars_deleted += 1;
            stats.bytes_deleted += bytes;
            continue;
        }

        tracing::info!(key = key.as_str(), bytes, "deleting orphan NAR");
        match s3.delete_object(key).await {
            Ok(_) => {
                stats.nars_deleted += 1;
                stats.bytes_deleted += bytes;
            }
            Err(e) => {
                tracing::error!(
                    key = key.as_str(),
                    error = %e,
                    "failed to delete orphan NAR"
                );
            }
        }
    }

    Ok(stats)
}

pub fn is_not_found(err: &anyhow::Error) -> bool {
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

    /// Default `LastModified` used by `list_xml` for keys whose timestamp
    /// the test doesn't care about. Far enough in the past that the
    /// freshness filter (`prune_start_at`) will treat them as deletable.
    const OLD_LAST_MODIFIED: &str = "2020-01-01T00:00:00.000Z";

    /// Default per-object size for tests that don't care about specific
    /// byte counts. Non-zero so any "bytes_deleted == 0" assertion would
    /// fail loud, but small enough that round numbers in tests remain
    /// readable.
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

    /// Like `list_xml` but lets the test set per-key `LastModified` values
    /// (RFC 3339, e.g. "2026-05-03T12:00:00.000Z").
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

    /// Like `list_xml` but lets the test set per-key sizes (in bytes).
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

    fn generation_list_xml(keys: &[&str]) -> String {
        list_xml(keys)
    }

    /// Cutoff used by sweep tests when the exact value doesn't matter:
    /// after every key emitted by `list_xml` (which uses
    /// `OLD_LAST_MODIFIED`) but before "now". 2026-01-01 fits that.
    fn default_prune_start_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
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

    /// Helper: build N copies of a 500 ReplayEvent. The SDK's default retry
    /// strategy makes up to 3 attempts on transient errors; provide a generous
    /// margin so the test exercises retry exhaustion deterministically.
    fn server_error_events(n: usize) -> Vec<ReplayEvent> {
        (0..n)
            .map(|_| {
                ReplayEvent::new(
                    empty_request(),
                    http::Response::builder()
                        .status(500)
                        .body(SdkBody::from("internal error"))
                        .unwrap(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn non_404_narinfo_error_aborts_mark_phase() {
        // Regression: a transient 5xx on a narinfo GET must NOT be classified
        // as Dead — that would make sweep_phase delete a live narinfo + NAR.
        let mut events = vec![
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
            // generation JSON
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(
                        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-pkg-a",
                    )))
                    .unwrap(),
            ),
        ];
        events.extend(server_error_events(5));
        let client = mock_client(events);

        let result = mark_phase(&client).await;
        assert!(
            result.is_err(),
            "non-404 narinfo fetch errors must abort mark_phase to prevent deleting live data"
        );
    }

    #[tokio::test]
    async fn non_404_generation_error_aborts_mark_phase() {
        // Regression: a transient 5xx on a generation JSON GET must NOT cause
        // the root to be silently dropped — that would delete the generation's
        // entire closure in sweep_phase.
        let mut events = vec![
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
        ];
        events.extend(server_error_events(5));
        let client = mock_client(events);

        let result = mark_phase(&client).await;
        assert!(
            result.is_err(),
            "non-404 generation JSON fetch errors must abort mark_phase to prevent deleting live data"
        );
    }

    // ── sweep_narinfos tests ───────────────────────────────────────

    #[tokio::test]
    async fn sweep_narinfos_deletes_dead_narinfos() {
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
            // delete_object("deaddeaddeaddeaddeaddeaddeaddead.narinfo") — only,
            // no NAR delete (sweep_nars handles NARs).
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
        assert_eq!(stats.bytes_deleted, DEFAULT_SIZE);
    }

    #[tokio::test]
    async fn sweep_narinfos_dry_run_counts_but_does_not_delete() {
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

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), true)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
        // Dry-run still tallies bytes that would have been deleted.
        assert_eq!(stats.bytes_deleted, DEFAULT_SIZE);
        // No delete_object calls were made — StaticReplayClient would
        // panic if unexpected requests were sent.
    }

    #[tokio::test]
    async fn sweep_narinfos_bytes_deleted_sums_per_object_size() {
        let live: HashSet<String> = HashSet::new();

        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml_with_sizes(&[
                        ("deaddeaddeaddeaddeaddeaddeaddead.narinfo", 1234),
                        ("ff00ff00ff00ff00ff00ff00ff00ff00.narinfo", 5678),
                    ])))
                    .unwrap(),
            ),
            // DELETE narinfo 1
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // DELETE narinfo 2
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_deleted, 2);
        assert_eq!(stats.bytes_deleted, 1234 + 5678);
    }

    #[tokio::test]
    async fn sweep_narinfos_skips_guardrail_prefixes() {
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
            // Only the non-guardrail dead narinfo is deleted.
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        // Both narinfos are counted as checked, but generations/ one is skipped
        assert_eq!(stats.narinfos_checked, 2);
        assert_eq!(stats.narinfos_deleted, 1);
    }

    #[tokio::test]
    async fn sweep_narinfos_does_not_touch_nars_when_dead_pair_exists() {
        // Phase isolation: even when a dead narinfo and its NAR both appear in
        // the listing, sweep_narinfos must only delete the narinfo. The NAR
        // (and the eventual decision to delete it) is sweep_nars' problem.
        // StaticReplayClient panics on extra requests; the absence of a NAR
        // DELETE event proves the isolation.
        let live: HashSet<String> = HashSet::new();

        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                        "nar/deaddeaddeaddeaddeaddeaddeaddead.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // Only the narinfo DELETE is replayed.
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 1);
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
        //
        // Bucket NARs (a, d, i, j only — others not uploaded yet to keep the
        // test scoped). After the full run we expect:
        //   - I.narinfo, J.narinfo deleted by sweep_narinfos
        //   - nar/I.nar.xz, nar/J.nar.xz deleted by sweep_nars (orphan)
        //   - nar/A.nar.xz, nar/D.nar.xz kept (referenced by live narinfos)
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

        // Listings used by the two phases. The narinfo-sweep listing includes
        // I and J (still in the bucket); the nar-sweep narinfo listing omits
        // them (sweep_narinfos has just deleted them in the real flow).
        let pre_sweep_listing = list_xml(&[
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
        ]);
        let post_narinfo_sweep_listing = list_xml(&[
            &format!("{a_hash}.narinfo"),
            &format!("{b_hash}.narinfo"),
            &format!("{c_hash}.narinfo"),
            &format!("{d_hash}.narinfo"),
            &format!("{e_hash}.narinfo"),
            &format!("{f_hash}.narinfo"),
            &format!("{g_hash}.narinfo"),
            &format!("{h_hash}.narinfo"),
            &format!("nar/{a_hash}.nar.xz"),
            &format!("nar/{d_hash}.nar.xz"),
            &format!("nar/{i_hash}.nar.xz"),
            &format!("nar/{j_hash}.nar.xz"),
            "generations/dom/1/gen-a.json",
        ]);
        let nar_listing = list_xml(&[
            &format!("nar/{a_hash}.nar.xz"),
            &format!("nar/{d_hash}.nar.xz"),
            &format!("nar/{i_hash}.nar.xz"),
            &format!("nar/{j_hash}.nar.xz"),
        ]);

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
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{a_hash}-pkg-a"
                    ))))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{d_hash}-pkg-d"
                    ))))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(generation_json(&format!(
                        "/nix/store/{f_hash}-pkg-f"
                    ))))
                    .unwrap(),
            ),
            // Wave 1: A.narinfo → refs [B, C]
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(a_hash, a_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(d_hash, d_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(f_hash, f_refs)))
                    .unwrap(),
            ),
            // Wave 2
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(b_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(c_hash, c_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(e_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(g_hash, g_refs)))
                    .unwrap(),
            ),
            // Wave 3
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(h_hash, no_refs)))
                    .unwrap(),
            ),
            // ── sweep_narinfos ──
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(pre_sweep_listing))
                    .unwrap(),
            ),
            // DELETE I.narinfo
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // DELETE J.narinfo
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // ── sweep_nars: URL collection ──
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(post_narinfo_sweep_listing))
                    .unwrap(),
            ),
            // GET each surviving narinfo for its URL (A..H, listing order).
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(a_hash, a_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(b_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(c_hash, c_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(d_hash, d_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(e_hash, no_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(f_hash, f_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(g_hash, g_refs)))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(narinfo_body(h_hash, no_refs)))
                    .unwrap(),
            ),
            // ── sweep_nars: NAR sweep ──
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(nar_listing))
                    .unwrap(),
            ),
            // DELETE nar/I.nar.xz (orphan)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // DELETE nar/J.nar.xz (orphan)
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        // Run all three phases the way `run` does with `command: None`.
        let live = mark_phase(&client).await.unwrap();
        let narinfo_stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        let nar_stats = sweep_nars(&client, default_prune_start_at(), false)
            .await
            .unwrap();

        // mark_phase: 8 live hashes
        assert_eq!(live.len(), 8);
        for h in [
            a_hash, b_hash, c_hash, d_hash, e_hash, f_hash, g_hash, h_hash,
        ] {
            assert!(live.contains(h));
        }
        for h in [i_hash, j_hash] {
            assert!(!live.contains(h));
        }

        // sweep_narinfos: only I and J deleted.
        assert_eq!(narinfo_stats.narinfos_checked, 10);
        assert_eq!(narinfo_stats.narinfos_deleted, 2);

        // sweep_nars: 2 NARs (A, D) referenced and kept; 2 NARs (I, J) orphan
        // and deleted.
        assert_eq!(nar_stats.narinfos_scanned, 8);
        assert_eq!(nar_stats.referenced_nars, 8);
        assert_eq!(nar_stats.nars_checked, 4);
        assert_eq!(nar_stats.nars_kept_referenced, 2);
        assert_eq!(nar_stats.nars_deleted, 2);
    }

    // ── sweep_narinfos freshness tests ─────────────────────────────

    #[tokio::test]
    async fn sweep_narinfos_skips_recently_modified_dead_narinfo() {
        // Dead-by-hash narinfo whose LastModified is *after* prune_start_at:
        // the freshness filter must skip it. StaticReplayClient has only the
        // listing event — any DELETE would panic as an unexpected request.
        let live: HashSet<String> = HashSet::new();
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(list_xml_with_times(&[(
                    "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                    "2026-05-03T12:00:00.000Z",
                )])))
                .unwrap(),
        )]);

        let cutoff = Utc.with_ymd_and_hms(2026, 5, 3, 11, 0, 0).unwrap();
        let stats = sweep_narinfos(&client, &live, cutoff, false).await.unwrap();
        assert_eq!(stats.narinfos_checked, 0);
        assert_eq!(stats.narinfos_deleted, 0);
    }

    #[tokio::test]
    async fn sweep_narinfos_deletes_old_dead_narinfo() {
        let live: HashSet<String> = HashSet::new();
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml_with_times(&[(
                        "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                        "2026-04-01T00:00:00.000Z",
                    )])))
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

        let cutoff = Utc.with_ymd_and_hms(2026, 5, 3, 0, 0, 0).unwrap();
        let stats = sweep_narinfos(&client, &live, cutoff, false).await.unwrap();
        assert_eq!(stats.narinfos_checked, 1);
        assert_eq!(stats.narinfos_deleted, 1);
    }

    #[tokio::test]
    async fn sweep_narinfos_skips_narinfo_with_missing_last_modified() {
        let live: HashSet<String> = HashSet::new();
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>deaddeaddeaddeaddeaddeaddeaddead.narinfo</Key></Contents>
</ListBucketResult>"#;
        let client = mock_client(vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(xml))
                .unwrap(),
        )]);

        let stats = sweep_narinfos(&client, &live, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_checked, 0);
        assert_eq!(stats.narinfos_deleted, 0);
    }

    // ── sweep_nars tests ───────────────────────────────────────────

    #[tokio::test]
    async fn sweep_nars_keeps_referenced_dedup() {
        // The CI symptom regression. Two narinfos A and B with identical
        // URL nar/X.nar.xz; only B is reachable from any generation root.
        // Even though A is dead (no longer in the bucket post-narinfo-sweep),
        // B's URL keeps nar/X.nar.xz alive. StaticReplayClient panics on
        // unexpected requests — the absence of a DELETE event for the NAR
        // is the assertion.
        let client = mock_client(vec![
            // list "" for narinfo URL collection
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.narinfo",
                        "nar/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // GET B.narinfo — its URL is the shared nar/X.nar.xz
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(format!(
                        "StorePath: /nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-pkg\n\
                         URL: nar/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.nar.xz\n\
                         References:\n"
                    )))
                    .unwrap(),
            ),
            // list "nar/" for orphan sweep
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "nar/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // No DELETE — the NAR is referenced by B.
        ]);

        let stats = sweep_nars(&client, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_scanned, 1);
        assert_eq!(stats.referenced_nars, 1);
        assert_eq!(stats.nars_checked, 1);
        assert_eq!(stats.nars_kept_referenced, 1);
        assert_eq!(stats.nars_deleted, 0);
    }

    #[tokio::test]
    async fn sweep_nars_deletes_orphan_nar() {
        // Empty narinfo set; one NAR exists. It's orphan → delete.
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "nar/orphan00000000000000000000000000.nar.xz",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "nar/orphan00000000000000000000000000.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // DELETE the orphan
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_nars(&client, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.narinfos_scanned, 0);
        assert_eq!(stats.referenced_nars, 0);
        assert_eq!(stats.nars_checked, 1);
        assert_eq!(stats.nars_deleted, 1);
        assert_eq!(stats.bytes_deleted, DEFAULT_SIZE);
    }

    #[tokio::test]
    async fn sweep_nars_bytes_deleted_sums_per_object_size() {
        let client = mock_client(vec![
            // No narinfos — every NAR is orphan.
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[])))
                    .unwrap(),
            ),
            // nar listing with explicit per-object sizes
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml_with_sizes(&[
                        ("nar/orphan00000000000000000000000000.nar.xz", 4096),
                        ("nar/orphan11111111111111111111111111.nar.xz", 8192),
                    ])))
                    .unwrap(),
            ),
            // DELETE orphan 1
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
            // DELETE orphan 2
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::empty())
                    .unwrap(),
            ),
        ]);

        let stats = sweep_nars(&client, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.nars_deleted, 2);
        assert_eq!(stats.bytes_deleted, 4096 + 8192);
    }

    #[tokio::test]
    async fn sweep_nars_keeps_dead_narinfos_referenced_nar() {
        // Phase isolation: a dead narinfo that sweep_narinfos hasn't yet
        // deleted (or failed to delete) still shows up in this phase's
        // narinfo scan and protects its NAR. Re-running fixes it; nothing
        // gets stranded mid-sweep.
        let client = mock_client(vec![
            // list "" for URL collection — dead narinfo still present
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "deaddeaddeaddeaddeaddeaddeaddead.narinfo",
                    ])))
                    .unwrap(),
            ),
            // GET the still-present dead narinfo
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
            // list "nar/"
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "nar/deaddeaddeaddeaddeaddeaddeaddead.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // No DELETE — the NAR is "referenced" (by the surviving dead narinfo)
        ]);

        let stats = sweep_nars(&client, default_prune_start_at(), false)
            .await
            .unwrap();
        assert_eq!(stats.nars_kept_referenced, 1);
        assert_eq!(stats.nars_deleted, 0);
    }

    #[tokio::test]
    async fn sweep_nars_skips_recently_modified_nar() {
        // In-flight push protection on the NAR side: a NAR uploaded after
        // the cutoff must not be deleted even when no narinfo references it
        // yet (the narinfo upload may not have landed).
        let client = mock_client(vec![
            // No narinfos
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[])))
                    .unwrap(),
            ),
            // Recent NAR
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml_with_times(&[(
                        "nar/recent00000000000000000000000000.nar.xz",
                        "2026-05-09T23:59:00.000Z",
                    )])))
                    .unwrap(),
            ),
            // No DELETE — freshness filter skipped it.
        ]);

        let cutoff = Utc.with_ymd_and_hms(2026, 5, 9, 23, 0, 0).unwrap();
        let stats = sweep_nars(&client, cutoff, false).await.unwrap();
        assert_eq!(stats.nars_checked, 0);
        assert_eq!(stats.nars_deleted, 0);
    }

    #[tokio::test]
    async fn sweep_nars_dry_run_does_not_delete() {
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "nar/orphan00000000000000000000000000.nar.xz",
                    ])))
                    .unwrap(),
            ),
            // No DELETE — dry run.
        ]);

        let stats = sweep_nars(&client, default_prune_start_at(), true)
            .await
            .unwrap();
        assert_eq!(stats.nars_checked, 1);
        assert_eq!(stats.nars_deleted, 1);
        // Dry-run still tallies bytes that would have been deleted.
        assert_eq!(stats.bytes_deleted, DEFAULT_SIZE);
    }

    #[tokio::test]
    async fn sweep_nars_aborts_on_unparseable_narinfo() {
        // A narinfo whose body fetches OK but doesn't parse must abort the
        // phase. Continuing would silently drop a referenced URL and risk
        // deleting a still-live NAR. Bucket has both a NAR and a corrupt
        // narinfo; absence of any DELETE event is part of the assertion
        // (StaticReplayClient panics on unexpected requests).
        let client = mock_client(vec![
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from(list_xml(&[
                        "corrupt000000000000000000000000.narinfo",
                    ])))
                    .unwrap(),
            ),
            ReplayEvent::new(
                empty_request(),
                http::Response::builder()
                    .status(200)
                    .body(SdkBody::from("not a real narinfo body"))
                    .unwrap(),
            ),
        ]);

        let result = sweep_nars(&client, default_prune_start_at(), false).await;
        assert!(
            result.is_err(),
            "unparseable narinfo must abort URL collection to avoid deleting live NARs"
        );
    }

    #[tokio::test]
    async fn sweep_nars_aborts_on_narinfo_get_5xx() {
        // A non-404 GET on a narinfo during URL collection must abort the
        // phase. Treating a transient error as "no URL contributed" would
        // delete live NARs.
        let mut events = vec![ReplayEvent::new(
            empty_request(),
            http::Response::builder()
                .status(200)
                .body(SdkBody::from(list_xml(&[
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.narinfo",
                ])))
                .unwrap(),
        )];
        events.extend(server_error_events(5));
        let client = mock_client(events);

        let result = sweep_nars(&client, default_prune_start_at(), false).await;
        assert!(
            result.is_err(),
            "non-404 narinfo GET errors during URL collection must abort to avoid deleting live NARs"
        );
    }

    // ── run() top-level guards ─────────────────────────────────────

    #[tokio::test]
    async fn run_rejects_combined_dry_run() {
        // No subcommand + --dry-run is rejected up-front: phase 1 wouldn't
        // delete narinfos, so phase 2's URL scan would still see them and
        // never report any orphan NAR. Erroring is more honest than
        // silently producing a misleading 0.
        let args = PruneArgs {
            command: None,
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:1".to_string()),
            dry_run: true,
            freshness_buffer_secs: DEFAULT_FRESHNESS_BUFFER_SECS,
        };
        let err = run(args).await.expect_err("combined dry-run must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("--dry-run") && msg.contains("subcommand"),
            "error should explain why combined dry-run is rejected, got: {msg}"
        );
    }
}
