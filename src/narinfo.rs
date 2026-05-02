use anyhow::{Result, anyhow};

/// Minimal parsed NARinfo — only the fields we need for closure traversal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Narinfo {
    pub store_path: String,
    pub references: Vec<String>,
    pub url: String,
}

/// Parse a NARinfo text body into a `Narinfo`.
///
/// The NARinfo format is a sequence of `Key: Value\n` lines. We extract only
/// `StorePath`, `References`, and `URL`; all other keys are silently ignored.
pub fn parse_narinfo(body: &str) -> Result<Narinfo> {
    let mut store_path = None;
    let mut references = None;
    let mut url = None;

    for line in body.lines() {
        let line = line.trim_end();

        if let Some((key, value)) = line.split_once(':') {
            // Value starts after the colon and optional space
            let value = value.strip_prefix(' ').unwrap_or(value);

            match key {
                "StorePath" => store_path = Some(value.to_string()),
                "References" => {
                    references = Some(if value.is_empty() {
                        Vec::new()
                    } else {
                        value.split_whitespace().map(String::from).collect()
                    })
                }
                "URL" => url = Some(value.to_string()),
                _ => {} // ignore NarHash, NarSize, Sig, Deriver, etc.
            }
        }
    }

    let store_path = store_path.ok_or_else(|| anyhow!("missing StorePath field"))?;
    let url = url.ok_or_else(|| anyhow!("missing URL field"))?;
    let references = references.unwrap_or_default();

    Ok(Narinfo {
        store_path,
        references,
        url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_NARINFO: &str = "\
StorePath: /nix/store/abc123-my-package
URL: nar/abc123.nar.xz
References: def456-dep1 ghi789-dep2
NarHash: sha256:xyz
NarSize: 4096
Sig: cache.example.org:ABCDEF";

    #[test]
    fn parse_standard_narinfo() {
        let info = parse_narinfo(VALID_NARINFO).unwrap();
        assert_eq!(info.store_path, "/nix/store/abc123-my-package");
        assert_eq!(info.url, "nar/abc123.nar.xz");
        assert_eq!(info.references, vec!["def456-dep1", "ghi789-dep2"]);
    }

    #[test]
    fn parse_empty_references() {
        let body = "\
StorePath: /nix/store/abc123-my-package
URL: nar/abc123.nar.xz
References:
NarHash: sha256:xyz";
        let info = parse_narinfo(body).unwrap();
        assert!(info.references.is_empty());
    }

    #[test]
    fn parse_self_reference() {
        let body = "\
StorePath: /nix/store/abc123-my-package
URL: nar/abc123.nar.xz
References: abc123-my-package";
        let info = parse_narinfo(body).unwrap();
        assert_eq!(info.references, vec!["abc123-my-package"]);
    }

    #[test]
    fn parse_different_url_compressions() {
        for (url_suffix, expected) in [
            ("nar/abc.nar.xz", "nar/abc.nar.xz"),
            ("nar/abc.nar.zst", "nar/abc.nar.zst"),
            ("nar/abc.nar", "nar/abc.nar"),
        ] {
            let body = format!("StorePath: /nix/store/abc123-my-package\nURL: {url_suffix}\n");
            let info = parse_narinfo(&body).unwrap();
            assert_eq!(info.url, expected);
        }
    }

    #[test]
    fn missing_url_returns_error() {
        let body = "\
StorePath: /nix/store/abc123-my-package
References: def456-dep1";
        let result = parse_narinfo(body);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("missing URL"));
    }

    #[test]
    fn missing_store_path_returns_error() {
        let body = "\
URL: nar/abc.nar.xz
References: def456-dep1";
        let result = parse_narinfo(body);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("missing StorePath")
        );
    }

    #[test]
    fn malformed_input_returns_error() {
        let result = parse_narinfo("");
        assert!(result.is_err());
    }

    #[test]
    fn ignores_unknown_fields() {
        let body = "\
StorePath: /nix/store/abc123-my-package
URL: nar/abc.nar.xz
Deriver: abc123-my-package.drv
NarHash: sha256:deadbeef
NarSize: 12345
Sig: cache.nixos.org:ABCDEF";
        let info = parse_narinfo(body).unwrap();
        assert_eq!(info.store_path, "/nix/store/abc123-my-package");
        assert_eq!(info.url, "nar/abc.nar.xz");
        assert!(info.references.is_empty());
    }
}
