use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// A validated `/nix/store/...` path. Construction via `TryFrom` enforces
/// the prefix and that the bytes are UTF-8 (Nix store paths are ASCII by
/// spec, so this always holds for real Nix output). Holding that invariant
/// in the type lets `as_str` / `as_path` be infallible at use sites instead
/// of every caller doing its own validation or `expect`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Deserialize)]
#[serde(try_from = "String")]
pub struct NixStorePath(String);

impl NixStorePath {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl fmt::Display for NixStorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<Path> for NixStorePath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl serde::Serialize for NixStorePath {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> std::result::Result<S::Ok, S::Error> {
        ser.serialize_str(&self.0)
    }
}

impl TryFrom<String> for NixStorePath {
    type Error = anyhow::Error;
    fn try_from(s: String) -> Result<Self> {
        if !s.starts_with("/nix/store/") {
            anyhow::bail!("not a /nix/store path: {s}");
        }
        Ok(Self(s))
    }
}

impl TryFrom<PathBuf> for NixStorePath {
    type Error = anyhow::Error;
    fn try_from(p: PathBuf) -> Result<Self> {
        let s = p
            .into_os_string()
            .into_string()
            .map_err(|os| anyhow::anyhow!("nix store path is not utf-8: {os:?}"))?;
        Self::try_from(s)
    }
}

/// Represents a Nix store path with its metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StorePath {
    pub path: NixStorePath,
    pub deriver: Option<NixStorePath>,
    pub ultimate: bool,
    pub references: Vec<NixStorePath>,
}

/// Value side of the `nix path-info --json` map. The store path itself is the
/// map key, so it isn't part of this struct. Unknown fields are ignored.
#[derive(Deserialize)]
struct PathInfoMeta {
    #[serde(default)]
    deriver: Option<NixStorePath>,
    #[serde(default)]
    ultimate: bool,
    #[serde(default)]
    references: Vec<NixStorePath>,
}

/// Parse `nix path-info --json --json-format 1` output into a set of
/// `StorePath`s. Format 1 is a flat `{path: meta}` map with no envelope
/// fields, so we deserialize the whole thing directly.
pub fn parse_path_info(json: &[u8]) -> Result<HashSet<StorePath>> {
    let raw: HashMap<NixStorePath, PathInfoMeta> =
        serde_json::from_slice(json).context("failed to parse nix path-info JSON")?;
    Ok(raw
        .into_iter()
        .map(|(path, meta)| StorePath {
            path,
            deriver: meta.deriver,
            ultimate: meta.ultimate,
            references: meta.references,
        })
        .collect())
}

/// Trait for running Nix CLI commands, allowing for mock implementations in tests.
pub trait NixCommandRunner: Send + Sync {
    /// Execute a `nix` command with the given arguments and return its output.
    fn run_nix(&self, args: &[&str]) -> Result<Output>;
}

/// Default implementation that shells out to the real `nix` CLI.
pub struct RealNixRunner;

impl NixCommandRunner for RealNixRunner {
    fn run_nix(&self, args: &[&str]) -> Result<Output> {
        // Always force-enable the experimental features we depend on, in case
        // the user's Nix installer didn't configure them in /etc/nix/nix.conf.
        // `--extra-experimental-features` is additive (unlike
        // `--experimental-features`), so it won't clobber other features the
        // host has enabled.
        let output = Command::new("nix")
            .arg("--extra-experimental-features")
            .arg("nix-command flakes")
            .args(args)
            .output()
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => {
                    anyhow::anyhow!("nix command not found: ensure Nix is installed and in PATH")
                }
                _ => anyhow::anyhow!("failed to execute nix command: {}", e),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "nix command failed with exit code {:?}: {}",
                output.status.code(),
                stderr.trim()
            ));
        }

        Ok(output)
    }
}

/// Returns a snapshot of all store paths by running `nix path-info --json --all`.
///
/// We pin `--json-format 1` so the output shape is the same `{path: meta}`
/// map regardless of what the installed Nix version's default is. (Some
/// versions/forks default to format 2, which wraps entries under `paths`.)
pub fn snapshot_store<R: NixCommandRunner>(runner: &R) -> Result<HashSet<StorePath>> {
    let output = runner
        .run_nix(&["path-info", "--json", "--json-format", "1", "--all"])
        .context("failed to snapshot store")?;

    parse_path_info(&output.stdout)
}

/// Returns paths present in `after` but not in `before`.
pub fn diff_snapshots(before: &HashSet<StorePath>, after: &HashSet<StorePath>) -> Vec<StorePath> {
    after.difference(before).cloned().collect()
}

/// Reduce a set of store paths to the minimal subset whose joint closure is
/// the same as the original set's joint closure. A path P is dropped if some
/// other path Q in the set has P in its transitive closure (so referencing Q
/// is enough). Returns the kept paths sorted lexicographically.
pub fn minimal_closure_cover(
    paths: &[StorePath],
    snapshot: &HashSet<StorePath>,
) -> Vec<NixStorePath> {
    let by_path: HashMap<&Path, &StorePath> =
        snapshot.iter().map(|p| (p.path.as_path(), p)).collect();

    // Per-path transitive closure (stored as the set of paths reachable via
    // `references`, including the path itself).
    let closures: HashMap<&Path, HashSet<&Path>> = paths
        .iter()
        .map(|p| {
            let mut visited: HashSet<&Path> = HashSet::new();
            let mut stack: Vec<&Path> = vec![p.path.as_path()];
            while let Some(curr) = stack.pop() {
                if !visited.insert(curr) {
                    continue;
                }
                if let Some(node) = by_path.get(curr) {
                    for r in &node.references {
                        stack.push(r.as_path());
                    }
                }
            }
            (p.path.as_path(), visited)
        })
        .collect();

    let mut cover: Vec<NixStorePath> = paths
        .iter()
        .filter(|p| {
            !closures.iter().any(|(q, q_closure)| {
                *q != p.path.as_path() && q_closure.contains(p.path.as_path())
            })
        })
        .map(|p| p.path.clone())
        .collect();
    cover.sort();
    cover
}

/// Build the Nix expression for the closure-root derivation. The output is a
/// single text file listing the cover paths; Nix's reference scanner picks
/// them up as runtime refs. `roots = map builtins.storePath paths` is the
/// only mechanism that propagates string contexts to the .drv as build-time
/// deps without re-importing (path literals would copy fresh; `nix store
/// add-file` doesn't run the reference scanner).
///
/// `/bin/sh` is bind-mounted into the sandbox by default on Linux, so we
/// don't need bash. `echo` and `>` are shell builtins, so we don't need
/// coreutils. The closure-root .drv's only build-time deps are the cover
/// paths themselves.
///
/// `system = builtins.currentSystem` lets the Nix evaluator pick its own
/// system rather than us hardcoding it Rust-side; available because we
/// already eval with `--impure` (required for `builtins.storePath`).
fn closure_root_expr(paths: &[NixStorePath]) -> String {
    let path_strings = paths
        .iter()
        .map(|p| format!("\"{}\"", p.as_str()))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        r#"let paths = [ {path_strings} ];
in builtins.derivation {{
  name = "nix-s3-generations-closure-root";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo ${{builtins.toString paths}} > $out" ];
  roots = map builtins.storePath paths;
}}
"#,
    )
}

/// Build a single Nix store path whose runtime references are the given
/// (already-deduplicated) paths. A single `nix copy` of this output walks
/// the joint closure exactly once.
pub fn build_closure_root<R: NixCommandRunner>(
    runner: &R,
    paths: &[NixStorePath],
) -> Result<NixStorePath> {
    let expr = closure_root_expr(paths);
    // `--impure` is required because the expression uses `builtins.storePath`,
    // which reads from the local store and is rejected in pure evaluation.
    let output = runner
        .run_nix(&[
            "build",
            "--impure",
            "--no-link",
            "--print-out-paths",
            "--expr",
            &expr,
        ])
        .context("failed to build closure root")?;
    let store_path = String::from_utf8(output.stdout)
        .context("invalid utf8 from nix build")?
        .trim()
        .to_string();
    NixStorePath::try_from(store_path).context("nix build did not return a valid /nix/store path")
}

/// Path of the post-build-hook this action installs. Must match
/// `POST_BUILD_HOOK_PATH` in `action/helpers.ts`. The path is intentionally
/// unique so we detect *our* hook specifically and not, say, an unrelated
/// hook a user has configured for other purposes.
pub const OUR_POST_BUILD_HOOK_PATH: &str = "/etc/nix/nix-s3-generations-post-build-hook.sh";

/// Invoke `nix copy --to <to_uri> <path>`. Used by the post-push
/// restoration pass to re-walk the closure and re-upload any narinfo a
/// racing prune deleted between the original push and the gen root upload.
/// Existing narinfos are HEAD-skipped by `nix copy`; only missing ones
/// are re-uploaded from the local store.
pub fn copy_paths<R: NixCommandRunner>(
    runner: &R,
    to_uri: &str,
    path: &NixStorePath,
) -> Result<()> {
    runner
        .run_nix(&["copy", "--to", to_uri, path.as_str()])
        .with_context(|| format!("nix copy to {to_uri} failed for {path}"))?;
    Ok(())
}

/// Returns true if the active Nix daemon's `post-build-hook` is the one this
/// action installs. Used purely to make logging accurate: when our hook is
/// set, `nix build` triggers it on success and the hook uploads the build's
/// closure, which can dominate the wall time of `nix build`.
pub fn our_post_build_hook_installed<R: NixCommandRunner>(runner: &R) -> bool {
    runner
        .run_nix(&["config", "show", "post-build-hook"])
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim() == OUR_POST_BUILD_HOOK_PATH)
        .unwrap_or(false)
}

/// Convenience wrapper using the real Nix runner.
pub fn snapshot_store_real() -> Result<HashSet<StorePath>> {
    snapshot_store(&RealNixRunner)
}

/// Convenience wrapper using the real Nix runner.
pub fn our_post_build_hook_installed_real() -> bool {
    our_post_build_hook_installed(&RealNixRunner)
}

/// Convenience wrapper using the real Nix runner.
pub fn build_closure_root_real(paths: &[NixStorePath]) -> Result<NixStorePath> {
    build_closure_root(&RealNixRunner, paths)
}

/// Convenience wrapper using the real Nix runner.
pub fn copy_paths_real(to_uri: &str, path: &NixStorePath) -> Result<()> {
    copy_paths(&RealNixRunner, to_uri, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Output;

    struct MockRunner {
        expected_args: Vec<String>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit_code: i32,
        simulate_not_found: bool,
    }

    impl MockRunner {
        fn success(stdout: &str) -> Self {
            Self {
                expected_args: Vec::new(),
                stdout: stdout.as_bytes().to_vec(),
                stderr: Vec::new(),
                exit_code: 0,
                simulate_not_found: false,
            }
        }

        fn failure(stderr: &str, exit_code: i32) -> Self {
            Self {
                expected_args: Vec::new(),
                stdout: Vec::new(),
                stderr: stderr.as_bytes().to_vec(),
                exit_code,
                simulate_not_found: false,
            }
        }

        fn not_found() -> Self {
            Self {
                expected_args: Vec::new(),
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: 0,
                simulate_not_found: true,
            }
        }

        fn with_expected_args(mut self, args: &[&str]) -> Self {
            self.expected_args = args.iter().map(|s| s.to_string()).collect();
            self
        }
    }

    impl NixCommandRunner for MockRunner {
        fn run_nix(&self, args: &[&str]) -> Result<Output> {
            if self.simulate_not_found {
                return Err(anyhow::anyhow!(
                    "nix command not found: ensure Nix is installed and in PATH"
                ));
            }

            if !self.expected_args.is_empty() {
                let actual: Vec<String> = args.iter().map(|s| s.to_string()).collect();
                assert_eq!(
                    actual, self.expected_args,
                    "nix command args did not match expected"
                );
            }

            if self.exit_code != 0 {
                let stderr = String::from_utf8_lossy(&self.stderr);
                return Err(anyhow::anyhow!(
                    "nix command failed with exit code {}: {}",
                    self.exit_code,
                    stderr.trim()
                ));
            }

            Ok(Output {
                status: std::process::ExitStatus::default(),
                stdout: self.stdout.clone(),
                stderr: self.stderr.clone(),
            })
        }
    }

    fn nsp(s: &str) -> NixStorePath {
        NixStorePath::try_from(s.to_string()).unwrap()
    }

    fn make_store_path(path: &str, ultimate: bool) -> StorePath {
        StorePath {
            path: nsp(path),
            deriver: None,
            ultimate,
            references: Vec::new(),
        }
    }

    #[test]
    fn test_snapshot_store_parses_json() {
        let json = r#"{
            "/nix/store/aaa": {"deriver": "/nix/store/aaa.drv", "ultimate": true, "references": ["/nix/store/bbb"]},
            "/nix/store/bbb": {"deriver": null, "ultimate": false, "references": []}
        }"#;
        let runner = MockRunner::success(json);
        let result = snapshot_store(&runner).unwrap();
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
    fn test_snapshot_store_nix_not_found() {
        let runner = MockRunner::not_found();
        let err = snapshot_store(&runner).unwrap_err();
        let chain: Vec<String> = err.chain().map(|e| e.to_string()).collect();
        assert!(
            chain.iter().any(|m| m.contains("not found")),
            "error chain should indicate nix not found: {:?}",
            chain
        );
    }

    #[test]
    fn test_snapshot_store_command_failed() {
        let runner = MockRunner::failure("some error", 1);
        let err = snapshot_store(&runner).unwrap_err();
        let chain: Vec<String> = err.chain().map(|e| e.to_string()).collect();
        assert!(
            chain.iter().any(|m| m.contains("some error")),
            "error chain should contain the nix stderr: {:?}",
            chain
        );
    }

    #[test]
    fn test_diff_snapshots() {
        let a = make_store_path("/nix/store/a", true);
        let b = make_store_path("/nix/store/b", false);
        let c = make_store_path("/nix/store/c", true);

        let before: HashSet<StorePath> = [a.clone(), b.clone()].into_iter().collect();
        let after: HashSet<StorePath> = [a.clone(), b.clone(), c.clone()].into_iter().collect();

        let diff = diff_snapshots(&before, &after);
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path.as_str(), "/nix/store/c");
    }

    #[test]
    fn test_closure_root_expr_uses_storepath_for_roots() {
        let expr = closure_root_expr(&[nsp("/nix/store/p1"), nsp("/nix/store/p2")]);
        assert!(expr.contains(r#"name = "nix-s3-generations-closure-root""#));
        // /bin/sh from the sandbox; no bash dep.
        assert!(expr.contains(r#"builder = "/bin/sh""#));
        // Cover paths appear as quoted strings in the local `paths` list...
        assert!(expr.contains(r#"paths = [ "/nix/store/p1" "/nix/store/p2" ]"#));
        // ...and are mapped through builtins.storePath for the `roots` attr,
        // which is what attaches their string contexts to the .drv.
        assert!(expr.contains("roots = map builtins.storePath paths"));
        // The script writes the path list to $out so the runtime scanner
        // turns them into runtime references.
        assert!(expr.contains("echo ${builtins.toString paths} > $out"));
    }

    #[test]
    fn nix_store_path_try_from_rejects_non_store_path() {
        let err = NixStorePath::try_from(PathBuf::from("/tmp/foo")).unwrap_err();
        assert!(err.to_string().contains("not a /nix/store path"));
    }

    #[test]
    fn nix_store_path_try_from_accepts_store_path() {
        let p = NixStorePath::try_from(PathBuf::from("/nix/store/abc-foo")).unwrap();
        assert_eq!(p.as_str(), "/nix/store/abc-foo");
    }

    fn store_path_with_refs(path: &str, refs: &[&str]) -> StorePath {
        StorePath {
            path: nsp(path),
            deriver: None,
            ultimate: true,
            references: refs.iter().copied().map(nsp).collect(),
        }
    }

    #[test]
    fn test_minimal_closure_cover_drops_paths_in_other_closures() {
        let a = store_path_with_refs("/nix/store/a", &["/nix/store/b"]);
        let b = store_path_with_refs("/nix/store/b", &["/nix/store/c"]);
        let c = store_path_with_refs("/nix/store/c", &[]);
        let snapshot: HashSet<StorePath> = [a.clone(), b.clone(), c.clone()].into_iter().collect();

        let cover = minimal_closure_cover(&[a, b, c], &snapshot);
        assert_eq!(cover, vec![nsp("/nix/store/a")]);
    }

    #[test]
    fn test_minimal_closure_cover_keeps_independent_roots() {
        let a = store_path_with_refs("/nix/store/a", &[]);
        let b = store_path_with_refs("/nix/store/b", &[]);
        let snapshot: HashSet<StorePath> = [a.clone(), b.clone()].into_iter().collect();

        let cover = minimal_closure_cover(&[a, b], &snapshot);
        assert_eq!(cover, vec![nsp("/nix/store/a"), nsp("/nix/store/b")]);
    }

    #[test]
    fn test_minimal_closure_cover_handles_diamond() {
        // a -> b, c; b -> d; c -> d
        let a = store_path_with_refs("/nix/store/a", &["/nix/store/b", "/nix/store/c"]);
        let b = store_path_with_refs("/nix/store/b", &["/nix/store/d"]);
        let c = store_path_with_refs("/nix/store/c", &["/nix/store/d"]);
        let d = store_path_with_refs("/nix/store/d", &[]);
        let snapshot: HashSet<StorePath> = [a.clone(), b.clone(), c.clone(), d.clone()]
            .into_iter()
            .collect();

        let cover = minimal_closure_cover(&[a, b, c, d], &snapshot);
        assert_eq!(cover, vec![nsp("/nix/store/a")]);
    }

    #[test]
    fn test_minimal_closure_cover_ignores_substituted_deps() {
        // Local: a, b. Snapshot also includes substituted dep s (not local).
        // a depends on s; b is independent. Cover should be {a, b}.
        let a = store_path_with_refs("/nix/store/a", &["/nix/store/s"]);
        let b = store_path_with_refs("/nix/store/b", &[]);
        let s = store_path_with_refs("/nix/store/s", &[]);
        let snapshot: HashSet<StorePath> = [a.clone(), b.clone(), s].into_iter().collect();

        let cover = minimal_closure_cover(&[a, b], &snapshot);
        assert_eq!(cover, vec![nsp("/nix/store/a"), nsp("/nix/store/b")]);
    }

    #[test]
    fn test_our_post_build_hook_installed_matches_exact_path() {
        let runner = MockRunner::success(&format!("{}\n", OUR_POST_BUILD_HOOK_PATH))
            .with_expected_args(&["config", "show", "post-build-hook"]);
        assert!(our_post_build_hook_installed(&runner));
    }

    #[test]
    fn test_our_post_build_hook_installed_rejects_other_hook() {
        let runner = MockRunner::success("/etc/nix/some-other-hook.sh\n");
        assert!(!our_post_build_hook_installed(&runner));
    }

    #[test]
    fn test_our_post_build_hook_installed_rejects_empty() {
        let runner = MockRunner::success("\n");
        assert!(!our_post_build_hook_installed(&runner));
    }

    #[test]
    fn test_our_post_build_hook_installed_handles_failure() {
        let runner = MockRunner::failure("config error", 1);
        assert!(!our_post_build_hook_installed(&runner));
    }

    #[test]
    fn test_copy_paths_invokes_nix_copy_with_uri_and_path() {
        let path = nsp("/nix/store/aaa-pkg");
        let uri = "s3://b?region=us-east-1&secret-key=/etc/nix/cache-priv-key.pem&compression=zstd";
        let runner =
            MockRunner::success("").with_expected_args(&["copy", "--to", uri, path.as_str()]);
        copy_paths(&runner, uri, &path).unwrap();
    }

    #[test]
    fn test_copy_paths_propagates_failure() {
        let path = nsp("/nix/store/aaa-pkg");
        let runner = MockRunner::failure("upload failed", 1);
        let err = copy_paths(&runner, "s3://b?region=us-east-1", &path).unwrap_err();
        let chain: Vec<String> = err.chain().map(|e| e.to_string()).collect();
        assert!(
            chain.iter().any(|m| m.contains("upload failed")),
            "error chain should contain nix stderr: {:?}",
            chain
        );
        assert!(
            chain.iter().any(|m| m.contains("nix copy")),
            "error chain should mention nix copy: {:?}",
            chain
        );
    }
}
