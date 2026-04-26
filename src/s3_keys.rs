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
}
