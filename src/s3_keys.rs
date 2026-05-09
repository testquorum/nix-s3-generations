//! S3 key path helpers.

use crate::nix::NixStorePath;

/// Build the S3 key for a generation root.
///
/// Format: `generations/{domain}/{generation}/{shard_id}.json`
///
/// - `domain` scopes a generational GC track and may contain `/` to
///   namespace under `{repo}/{workflow}` etc.
/// - `generation` is the numeric generation within the domain. Putting it
///   above the shard means `aws s3 ls` lists generations in numeric order.
/// - `shard_id` is the leaf filename (caller-validated to contain no `/`),
///   so the GC can list a generation's directory and recover its individual
///   entries unambiguously.
///
/// Components are inserted verbatim. S3 keys accept any UTF-8, so we don't
/// slugify — slugifying would risk collisions (e.g. `"foo bar"` and
/// `"foo-bar"` mapping to the same key).
pub fn generations_key(domain: &str, generation: u64, shard_id: &str) -> String {
    format!("generations/{domain}/{generation}/{shard_id}.json")
}

/// Inverse of [`generations_key`]: given an S3 key, recover its
/// `(domain, generation, shard_id)` components. Returns `None` if the key
/// does not match `generations/{domain}/{generation}/{shard_id}.json` or
/// the generation segment is not a `u64`.
///
/// `domain` may itself contain `/`, so we peel `shard_id` and `generation`
/// off the right; everything left is the domain.
pub fn parse_generation_key(key: &str) -> Option<(String, u64, String)> {
    let rest = key.strip_prefix("generations/")?;
    let stem = rest.strip_suffix(".json")?;

    let (head, shard_id) = stem.rsplit_once('/')?;
    let (domain, generation) = head.rsplit_once('/')?;

    if domain.is_empty() || shard_id.is_empty() {
        return None;
    }

    let generation: u64 = generation.parse().ok()?;
    Some((domain.to_string(), generation, shard_id.to_string()))
}

/// Build the narinfo S3 key for a Nix store path: the 32-character base32
/// hash prefix of the store path's basename, with `.narinfo` appended.
/// Example: `/nix/store/x4ay…-nix-s3-generations-closure-root` -> `x4ay….narinfo`.
pub fn narinfo_key(store_path: &NixStorePath) -> anyhow::Result<String> {
    use anyhow::Context;
    // `NixStorePath` always begins with `/nix/store/`, so there's at least
    // one '/'; the basename is whatever follows the last one.
    let basename = store_path
        .as_str()
        .rsplit_once('/')
        .map(|(_, b)| b)
        .unwrap_or_default();
    let (hash, _) = basename
        .split_once('-')
        .with_context(|| format!("store path basename has no hash-name separator: {basename}"))?;
    Ok(format!("{hash}.narinfo"))
}

/// Convert a References-format hash name (e.g., `x4ay-bash-5.2`) to a narinfo key
/// by splitting on the first `-` and appending `.narinfo`.
///
/// This takes the SHORT format (like `x4ay-bash-5.2`) used in NARinfo References
/// (space-separated hash-name strings) and converts it to the S3 key format.
pub fn hash_name_to_narinfo_key(hash_name: &str) -> String {
    let (hash, _) = hash_name.split_once('-').unwrap_or((hash_name, ""));
    format!("{hash}.narinfo")
}

/// Returns true if the S3 key is a narinfo file (ends with `.narinfo`).
pub fn is_narinfo_key(key: &str) -> bool {
    key.ends_with(".narinfo")
}

/// Top-level prefix for non-data state: prune coordination markers,
/// schema version files, audit logs, etc. Anything under `meta/` is
/// off-limits to the sweep guardrail (see `is_meta_key`).
pub const META_PREFIX: &str = "meta/";

/// S3 key for a prune attempt marker:
/// `meta/prune-attempts/{attempt_id}.json`. Mutable while the attempt is
/// running; rewritten exactly once when the attempt finalizes.
pub fn prune_attempt_key(attempt_id: &str) -> String {
    format!("meta/prune-attempts/{attempt_id}.json")
}

/// Listing prefix for a single attempt's conflict files.
/// `meta/prune-attempts/{attempt_id}/conflicts/`.
pub fn prune_attempt_conflicts_prefix(attempt_id: &str) -> String {
    format!("meta/prune-attempts/{attempt_id}/conflicts/")
}

/// S3 key for a single push conflict notice within an attempt:
/// `meta/prune-attempts/{attempt_id}/conflicts/{push_id}.json`.
pub fn prune_conflict_key(attempt_id: &str, push_id: &str) -> String {
    format!("meta/prune-attempts/{attempt_id}/conflicts/{push_id}.json")
}

/// Listing prefix for all prune attempt markers.
/// `meta/prune-attempts/`. Note: matches both the marker JSON (e.g.
/// `meta/prune-attempts/{id}.json`) and any per-attempt subdirectory
/// (e.g. `meta/prune-attempts/{id}/conflicts/...`); callers filter by
/// `.ends_with(".json")` and the absence of `/conflicts/` to isolate
/// just the attempt markers.
pub const PRUNE_ATTEMPTS_PREFIX: &str = "meta/prune-attempts/";

/// Returns true iff the given key looks like a top-level prune attempt
/// marker (i.e. directly under `meta/prune-attempts/`, ends `.json`, and
/// does not have an extra `/` indicating it's actually a conflict file
/// nested below).
pub fn is_prune_attempt_marker_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix(PRUNE_ATTEMPTS_PREFIX) else {
        return false;
    };
    rest.ends_with(".json") && !rest.contains('/')
}

/// Inverse of [`prune_conflict_key`]: given a conflict file's S3 key,
/// recover the parent `attempt_id`. Returns `None` if the key isn't shaped
/// like a conflict file under `meta/prune-attempts/{id}/conflicts/...`.
pub fn parse_conflict_key_attempt_id(key: &str) -> Option<&str> {
    let rest = key.strip_prefix(PRUNE_ATTEMPTS_PREFIX)?;
    let (attempt_id, after) = rest.split_once('/')?;
    if !after.starts_with("conflicts/") {
        return None;
    }
    Some(attempt_id)
}

/// Returns true if the S3 key is under the `meta/` coordination prefix.
/// The sweep guardrail uses this to ensure prune never deletes its own
/// coordination state.
pub fn is_meta_key(key: &str) -> bool {
    key.starts_with(META_PREFIX)
}

/// Validate and return a NAR URL.
///
/// - Rejects path traversal sequences (`..`)
/// - Rejects absolute URLs (`http://` or `https://`)
/// - Ensures the path starts with `nar/`
pub fn sanitize_nar_url(url: &str) -> anyhow::Result<String> {
    // Reject empty string
    anyhow::ensure!(!url.is_empty(), "NAR URL cannot be empty");

    // Reject absolute URLs
    anyhow::ensure!(
        !url.starts_with("http://") && !url.starts_with("https://"),
        "NAR URL must not be an absolute HTTP URL: {url}"
    );

    // Reject path traversal
    anyhow::ensure!(
        !url.contains(".."),
        "NAR URL must not contain path traversal: {url}"
    );

    // Ensure starts with nar/
    if !url.starts_with("nar/") {
        anyhow::bail!("NAR URL must start with 'nar/': {url}");
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nsp(s: &str) -> NixStorePath {
        NixStorePath::try_from(s.to_string()).unwrap()
    }

    #[test]
    fn generations_key_formats_correctly() {
        assert_eq!(
            generations_key("owner/repo/ci", 42, "GitHub Actions 3"),
            "generations/owner/repo/ci/42/GitHub Actions 3.json"
        );
    }

    #[test]
    fn generations_key_default_shard() {
        assert_eq!(
            generations_key("standalone", 1, "gen"),
            "generations/standalone/1/gen.json"
        );
    }

    #[test]
    fn generations_key_shards_in_same_generation_do_not_overwrite() {
        let a = generations_key("owner/repo/ci", 42, "GitHub Actions 1");
        let b = generations_key("owner/repo/ci", 42, "GitHub Actions 2");
        assert_ne!(a, b);
    }

    #[test]
    fn generations_key_different_generations_do_not_overwrite() {
        let a = generations_key("owner/repo/ci", 42, "gen");
        let b = generations_key("owner/repo/ci", 43, "gen");
        assert_ne!(a, b);
    }

    #[test]
    fn generations_key_preserves_shard_id_verbatim() {
        // Distinct shard ids can't collide via slugification.
        let with_space = generations_key("d", 1, "foo bar");
        let with_dash = generations_key("d", 1, "foo-bar");
        assert_ne!(with_space, with_dash);
    }

    // Tests for parse_generation_key

    #[test]
    fn parse_generation_key_simple() {
        let (domain, generation, shard) =
            parse_generation_key("generations/standalone/1/gen.json").unwrap();
        assert_eq!(domain, "standalone");
        assert_eq!(generation, 1);
        assert_eq!(shard, "gen");
    }

    #[test]
    fn parse_generation_key_domain_with_slashes() {
        let (domain, generation, shard) =
            parse_generation_key("generations/owner/repo/ci/42/GitHub Actions 3.json").unwrap();
        assert_eq!(domain, "owner/repo/ci");
        assert_eq!(generation, 42);
        assert_eq!(shard, "GitHub Actions 3");
    }

    #[test]
    fn parse_generation_key_round_trip() {
        let key = generations_key("owner/repo/ci", 42, "shard 1");
        let (domain, generation, shard) = parse_generation_key(&key).unwrap();
        assert_eq!(domain, "owner/repo/ci");
        assert_eq!(generation, 42);
        assert_eq!(shard, "shard 1");
    }

    #[test]
    fn parse_generation_key_rejects_wrong_prefix() {
        assert!(parse_generation_key("nar/abc.json").is_none());
        assert!(parse_generation_key("d/1/g.json").is_none());
    }

    #[test]
    fn parse_generation_key_rejects_wrong_suffix() {
        assert!(parse_generation_key("generations/d/1/g.txt").is_none());
        assert!(parse_generation_key("generations/d/1/g").is_none());
    }

    #[test]
    fn parse_generation_key_rejects_non_numeric_generation() {
        assert!(parse_generation_key("generations/d/notanumber/g.json").is_none());
    }

    #[test]
    fn parse_generation_key_rejects_missing_components() {
        // No generation+shard segments after the domain.
        assert!(parse_generation_key("generations/d.json").is_none());
        // Empty domain.
        assert!(parse_generation_key("generations//1/g.json").is_none());
    }

    #[test]
    fn narinfo_key_extracts_hash_prefix() {
        let p = nsp("/nix/store/x4ayiscwbhcj89ija7s294jrdjss4009-nix-s3-generations-closure-root");
        assert_eq!(
            narinfo_key(&p).unwrap(),
            "x4ayiscwbhcj89ija7s294jrdjss4009.narinfo"
        );
    }

    #[test]
    fn narinfo_key_handles_drv_basename() {
        let p = nsp("/nix/store/abc-pkg-1.0.drv");
        assert_eq!(narinfo_key(&p).unwrap(), "abc.narinfo");
    }

    #[test]
    fn narinfo_key_errors_on_basename_without_separator() {
        let p = nsp("/nix/store/nohyphen");
        assert!(narinfo_key(&p).is_err());
    }

    // Tests for hash_name_to_narinfo_key

    #[test]
    fn hash_name_to_narinfo_key_simple() {
        assert_eq!(hash_name_to_narinfo_key("x4ay-bash-5.2"), "x4ay.narinfo");
    }

    #[test]
    fn hash_name_to_narinfo_key_multiple_hyphens() {
        assert_eq!(hash_name_to_narinfo_key("abc-pkg-1.0"), "abc.narinfo");
    }

    #[test]
    fn hash_name_to_narinfo_key_single_hyphen() {
        assert_eq!(hash_name_to_narinfo_key("x4ay-nix"), "x4ay.narinfo");
    }

    #[test]
    fn hash_name_to_narinfo_key_no_hyphen() {
        // No hyphen - hash is the entire string
        assert_eq!(hash_name_to_narinfo_key("nohyphen"), "nohyphen.narinfo");
    }

    // Tests for is_narinfo_key

    #[test]
    fn is_narinfo_key_true_for_narinfo() {
        assert!(is_narinfo_key("x4ayiscwbhcj89ija7s294jrdjss4009.narinfo"));
    }

    #[test]
    fn is_narinfo_key_false_for_other_extensions() {
        assert!(!is_narinfo_key("x4ay.nar"));
        assert!(!is_narinfo_key("x4ay.json"));
        assert!(!is_narinfo_key("x4ay.txt"));
    }

    #[test]
    fn is_narinfo_key_false_for_empty_string() {
        assert!(!is_narinfo_key(""));
    }

    #[test]
    fn is_narinfo_key_false_for_path_with_narinfo_in_middle() {
        assert!(!is_narinfo_key("nar/x4ay.narinfo/file"));
    }

    // Tests for sanitize_nar_url

    #[test]
    fn sanitize_nar_url_valid() {
        assert_eq!(
            sanitize_nar_url("nar/abc.nar.xz").unwrap(),
            "nar/abc.nar.xz"
        );
        assert_eq!(
            sanitize_nar_url("nar/x4ayiscwbhcj89ija7s294jrdjss4009.narinfo").unwrap(),
            "nar/x4ayiscwbhcj89ija7s294jrdjss4009.narinfo"
        );
    }

    #[test]
    fn sanitize_nar_url_rejects_path_traversal() {
        assert!(sanitize_nar_url("../../etc/passwd").is_err());
        assert!(sanitize_nar_url("nar/../etc/passwd").is_err());
        assert!(sanitize_nar_url("nar/foo/../bar").is_err());
    }

    #[test]
    fn sanitize_nar_url_rejects_absolute_http_url() {
        assert!(sanitize_nar_url("http://evil.com/nar/abc.narinfo").is_err());
        assert!(sanitize_nar_url("https://evil.com/nar/abc.narinfo").is_err());
    }

    #[test]
    fn sanitize_nar_url_rejects_empty_string() {
        assert!(sanitize_nar_url("").is_err());
    }

    #[test]
    fn sanitize_nar_url_rejects_missing_nar_prefix() {
        assert!(sanitize_nar_url("abc.narinfo").is_err());
        assert!(sanitize_nar_url("file.narinfo").is_err());
    }

    // ── meta/ coordination key tests ────────────────────────────────

    #[test]
    fn prune_attempt_key_format() {
        assert_eq!(
            prune_attempt_key("20260509T120000.000000000Z-deadbeef"),
            "meta/prune-attempts/20260509T120000.000000000Z-deadbeef.json"
        );
    }

    #[test]
    fn prune_conflict_key_format() {
        assert_eq!(
            prune_conflict_key("attempt1", "push1"),
            "meta/prune-attempts/attempt1/conflicts/push1.json"
        );
    }

    #[test]
    fn prune_attempt_conflicts_prefix_ends_with_slash() {
        assert_eq!(
            prune_attempt_conflicts_prefix("attempt1"),
            "meta/prune-attempts/attempt1/conflicts/"
        );
    }

    #[test]
    fn is_meta_key_recognises_meta_prefix() {
        assert!(is_meta_key("meta/prune-attempts/abc.json"));
        assert!(is_meta_key("meta/foo"));
        assert!(is_meta_key("meta/"));
        assert!(!is_meta_key("generations/dom/1/g.json"));
        assert!(!is_meta_key("abc.narinfo"));
        assert!(!is_meta_key("nar/abc.nar.xz"));
    }

    #[test]
    fn parse_conflict_key_attempt_id_round_trip() {
        let key = prune_conflict_key("attemptX", "pushY");
        assert_eq!(parse_conflict_key_attempt_id(&key), Some("attemptX"));
    }

    #[test]
    fn parse_conflict_key_attempt_id_rejects_non_conflict_paths() {
        assert_eq!(
            parse_conflict_key_attempt_id("meta/prune-attempts/abc.json"),
            None
        );
        assert_eq!(parse_conflict_key_attempt_id("nar/abc.nar.xz"), None);
        assert_eq!(
            parse_conflict_key_attempt_id("meta/prune-attempts/abc/other/x.json"),
            None
        );
    }

    #[test]
    fn is_prune_attempt_marker_key_recognises_top_level_jsons_only() {
        // Top-level marker JSONs.
        assert!(is_prune_attempt_marker_key("meta/prune-attempts/abc.json"));
        // Conflict files are nested under {id}/conflicts/ — not markers.
        assert!(!is_prune_attempt_marker_key(
            "meta/prune-attempts/abc/conflicts/p.json"
        ));
        // Other prefixes don't match.
        assert!(!is_prune_attempt_marker_key("generations/dom/1/g.json"));
        // Wrong extension.
        assert!(!is_prune_attempt_marker_key("meta/prune-attempts/abc.txt"));
        // Bare prefix.
        assert!(!is_prune_attempt_marker_key("meta/prune-attempts/"));
    }
}
