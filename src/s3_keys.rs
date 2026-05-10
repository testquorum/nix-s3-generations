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

/// Returns true if the S3 key matches the NAR layout `nar/<hash>.nar[.<ext>]`.
///
/// Used by `prune nar` to positively filter the bucket listing before
/// considering a key for orphan deletion. Anything outside this shape is
/// not something prune created and not something prune should touch — even
/// if it lives under `nar/`. We require a non-empty `<hash>` segment, no
/// embedded `/` after the prefix, and either a bare `.nar` suffix or
/// `.nar.<non-empty-ext>`.
pub fn is_nar_key(key: &str) -> bool {
    let Some(rest) = key.strip_prefix("nar/") else {
        return false;
    };
    if rest.contains('/') {
        return false;
    }
    let Some((hash, suffix)) = rest.split_once('.') else {
        return false;
    };
    if hash.is_empty() {
        return false;
    }
    // suffix is everything after the first '.'. Accept either "nar" alone or
    // "nar.<ext>" with a non-empty extension. `.len() > 4` rejects "nar." (a
    // trailing dot with no extension) since "nar." is 4 chars.
    suffix == "nar" || (suffix.starts_with("nar.") && suffix.len() > 4)
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

    // Tests for is_nar_key

    #[test]
    fn is_nar_key_accepts_uncompressed() {
        assert!(is_nar_key(
            "nar/0wab6kssgzmc47dx8qnc9mq2xbgvdbjwv3500yxsxkf68faaan9y.nar"
        ));
    }

    #[test]
    fn is_nar_key_accepts_xz() {
        assert!(is_nar_key(
            "nar/0wab6kssgzmc47dx8qnc9mq2xbgvdbjwv3500yxsxkf68faaan9y.nar.xz"
        ));
    }

    #[test]
    fn is_nar_key_accepts_zstd_and_other_compressions() {
        assert!(is_nar_key("nar/abc.nar.zst"));
        assert!(is_nar_key("nar/abc.nar.bz2"));
        assert!(is_nar_key("nar/abc.nar.gz"));
        assert!(is_nar_key("nar/abc.nar.br"));
    }

    #[test]
    fn is_nar_key_rejects_subdirectory() {
        assert!(!is_nar_key("nar/foo/bar.nar.xz"));
    }

    #[test]
    fn is_nar_key_rejects_wrong_prefix() {
        assert!(!is_nar_key("narrr/x.nar"));
        assert!(!is_nar_key("abc.nar.xz"));
        assert!(!is_nar_key("/nar/abc.nar.xz"));
    }

    #[test]
    fn is_nar_key_rejects_wrong_suffix() {
        assert!(!is_nar_key("nar/abc.txt"));
        assert!(!is_nar_key("nar/abc.narinfo"));
        assert!(!is_nar_key("nar/abc.tar.xz"));
    }

    #[test]
    fn is_nar_key_rejects_empty_or_partial() {
        assert!(!is_nar_key(""));
        assert!(!is_nar_key("nar/"));
        assert!(!is_nar_key("nar/abc"));
        assert!(!is_nar_key("nar/.nar"));
        assert!(!is_nar_key("nar/.nar.xz"));
        assert!(!is_nar_key("nar/abc.nar."));
    }
}
