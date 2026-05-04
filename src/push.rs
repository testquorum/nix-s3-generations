use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use crate::generation::GenerationRoot;
use crate::nix::{self, StorePath};
use crate::s3::S3Client;
use crate::s3_keys;

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

fn load_snapshot(path: &Path) -> Result<HashSet<StorePath>> {
    let content = std::fs::read(path)
        .with_context(|| format!("failed to read snapshot file: {}", path.display()))?;
    nix::parse_path_info(&content)
        .with_context(|| format!("failed to parse snapshot JSON from: {}", path.display()))
}

/// True for AWS-native S3 endpoints, where Nix's `s3://` substituter URI
/// must omit the `endpoint=` parameter (the SDK's default endpoint
/// resolution applies). Mirrors `isNativeAws` in
/// `action/helpers.ts:buildNixS3Params`.
fn is_native_aws_endpoint(endpoint: &str) -> bool {
    if endpoint == "s3.amazonaws.com" {
        return true;
    }
    if let Some(rest) = endpoint.strip_prefix("s3.")
        && let Some(region) = rest.strip_suffix(".amazonaws.com")
    {
        return !region.is_empty() && !region.contains('.');
    }
    false
}

/// Build the destination URI for `nix copy --to` used by the restoration
/// pass. No `secret-key` is appended: the restoration pass only re-uploads
/// narinfos a racing prune deleted, and those paths were already signed
/// by the post-build hook on their original upload (the hook is required
/// to be installed — see the bail at the top of `run`). `nix copy`
/// without `secret-key` preserves the existing local-store signatures
/// rather than replacing them.
fn build_copy_uri(bucket: &str, region: &str, endpoint: Option<&str>) -> String {
    let s3_params = match endpoint {
        Some(ep) if !is_native_aws_endpoint(ep) => format!("endpoint={ep}&region={region}"),
        _ => format!("region={region}"),
    };
    format!("s3://{bucket}?{s3_params}&compression=zstd")
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

    let s3_client = S3Client::new(
        args.bucket.clone(),
        args.region.clone(),
        args.endpoint.clone(),
    )
    .await
    .context("failed to create S3 client")?;

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

    let generation_root = GenerationRoot::new(closure_root.clone());

    let gen_root_json = serde_json::to_vec_pretty(&generation_root)
        .context("failed to serialize generation root")?;

    let gen_key = s3_keys::generations_key(&args.domain, args.generation, &args.shard_id);
    s3_client
        .upload_object(&gen_key, gen_root_json, Some("application/json"))
        .await
        .context("failed to upload generation root")?;

    tracing::info!(key = %gen_key, "generation root uploaded");

    // Restoration pass. A prune whose mark phase ran before this run's gen
    // root JSON landed will not include this run's closure in its live set;
    // its sweep then deletes any narinfo whose `LastModified` predates
    // `prune_start_at - freshness_buffer_secs`. Re-running `nix copy` on
    // the closure root walks the closure, HEADs each path, and re-uploads
    // any narinfo that has gone missing — restoring the cache to the
    // state the gen root claims. Existing narinfos are skipped, so this
    // doesn't refresh `LastModified` on healthy paths.
    let copy_uri = build_copy_uri(&args.bucket, &args.region, args.endpoint.as_deref());
    tracing::info!(
        %closure_root,
        "restoration pass: re-running nix copy on closure root to re-upload any narinfos a racing prune deleted",
    );
    nix::copy_paths_real(&copy_uri, &closure_root)
        .context("restoration pass failed: nix copy on closure root after gen root upload")?;
    tracing::info!("Push complete. Closure restoration pass complete.");

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

    // Mirrors `buildNixS3Params` cases in `action/__tests__/nix-conf.test.ts`.
    // The two implementations must agree because the post-build hook (action
    // side) and the restoration pass (this binary) hand the resulting URI
    // to the same `nix copy --to`.

    #[test]
    fn is_native_aws_endpoint_global_endpoint() {
        assert!(is_native_aws_endpoint("s3.amazonaws.com"));
    }

    #[test]
    fn is_native_aws_endpoint_regional_endpoints() {
        assert!(is_native_aws_endpoint("s3.us-east-1.amazonaws.com"));
        assert!(is_native_aws_endpoint("s3.eu-west-1.amazonaws.com"));
    }

    #[test]
    fn is_native_aws_endpoint_rejects_third_party() {
        assert!(!is_native_aws_endpoint("abc.r2.cloudflarestorage.com"));
        assert!(!is_native_aws_endpoint("minio.local"));
        assert!(!is_native_aws_endpoint(""));
    }

    #[test]
    fn is_native_aws_endpoint_rejects_subdomain_lookalikes() {
        // `s3.foo.bar.amazonaws.com` is not the regional form; the region
        // segment must not contain dots.
        assert!(!is_native_aws_endpoint("s3.foo.bar.amazonaws.com"));
        // No region between `s3.` and `.amazonaws.com`.
        assert!(!is_native_aws_endpoint("s3..amazonaws.com"));
    }

    #[test]
    fn build_copy_uri_native_aws_omits_endpoint_param() {
        let uri = build_copy_uri("my-bucket", "us-east-1", Some("s3.amazonaws.com"));
        assert_eq!(uri, "s3://my-bucket?region=us-east-1&compression=zstd");
    }

    #[test]
    fn build_copy_uri_regional_aws_omits_endpoint_param() {
        let uri = build_copy_uri("b", "eu-west-1", Some("s3.eu-west-1.amazonaws.com"));
        assert_eq!(uri, "s3://b?region=eu-west-1&compression=zstd");
    }

    #[test]
    fn build_copy_uri_no_endpoint_omits_endpoint_param() {
        let uri = build_copy_uri("b", "us-east-1", None);
        assert_eq!(uri, "s3://b?region=us-east-1&compression=zstd");
    }

    #[test]
    fn build_copy_uri_third_party_includes_endpoint_param() {
        let uri = build_copy_uri("b", "auto", Some("abc.r2.cloudflarestorage.com"));
        assert_eq!(
            uri,
            "s3://b?endpoint=abc.r2.cloudflarestorage.com&region=auto&compression=zstd"
        );
    }
}
