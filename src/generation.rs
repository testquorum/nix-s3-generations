use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::nix::NixStorePath;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GenerationRoot {
    pub version: u32,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "storePath")]
    pub store_path: NixStorePath,
}

impl GenerationRoot {
    pub fn new(store_path: NixStorePath) -> Self {
        Self {
            version: 1,
            timestamp: Utc::now(),
            store_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn nsp(s: &str) -> NixStorePath {
        NixStorePath::try_from(s.to_string()).unwrap()
    }

    #[test]
    fn generation_root_serializes_to_json() {
        let root = GenerationRoot {
            version: 1,
            timestamp: Utc.with_ymd_and_hms(2026, 4, 26, 12, 0, 0).unwrap(),
            store_path: nsp("/nix/store/abc-closure-root"),
        };

        let json = serde_json::to_string_pretty(&root).unwrap();
        assert!(json.contains("\"version\": 1"));
        assert!(json.contains("\"timestamp\": \"2026-04-26T12:00:00Z\""));
        assert!(json.contains("/nix/store/abc-closure-root"));
    }

    #[test]
    fn generation_root_deserializes_from_json() {
        let json = r#"{
            "version": 1,
            "timestamp": "2026-04-26T12:00:00Z",
            "storePath": "/nix/store/abc-closure-root"
        }"#;

        let root: GenerationRoot = serde_json::from_str(json).unwrap();
        assert_eq!(root.version, 1);
        assert_eq!(
            root.timestamp,
            Utc.with_ymd_and_hms(2026, 4, 26, 12, 0, 0).unwrap()
        );
        assert_eq!(root.store_path.as_str(), "/nix/store/abc-closure-root");
    }

    #[test]
    fn generation_root_new_sets_version_and_timestamp() {
        let root = GenerationRoot::new(nsp("/nix/store/foo"));
        assert_eq!(root.version, 1);
        assert_eq!(root.store_path.as_str(), "/nix/store/foo");
        // timestamp should be close to now
        let now = Utc::now();
        let diff = now.signed_duration_since(root.timestamp);
        assert!(diff.num_seconds() < 5);
    }

    #[test]
    fn generation_root_round_trip() {
        let original = GenerationRoot {
            version: 1,
            timestamp: Utc.with_ymd_and_hms(2026, 4, 26, 12, 0, 0).unwrap(),
            store_path: nsp("/nix/store/abc"),
        };

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: GenerationRoot = serde_json::from_str(&json).unwrap();
        assert_eq!(original.version, deserialized.version);
        assert_eq!(original.timestamp, deserialized.timestamp);
        assert_eq!(original.store_path, deserialized.store_path);
    }

    #[test]
    fn generation_root_rejects_malformed_json() {
        let result = serde_json::from_str::<GenerationRoot>("not json at all");
        assert!(result.is_err());
    }

    #[test]
    fn generation_root_rejects_missing_fields() {
        let json = r#"{ "version": 1 }"#;
        let result = serde_json::from_str::<GenerationRoot>(json);
        assert!(result.is_err());
    }

    #[test]
    fn generation_root_rejects_non_store_path() {
        let json = r#"{
            "version": 1,
            "timestamp": "2026-04-26T12:00:00Z",
            "storePath": "/tmp/not-a-store-path"
        }"#;
        let result = serde_json::from_str::<GenerationRoot>(json);
        assert!(result.is_err());
    }

    #[test]
    fn generation_root_deserializes_version_one() {
        let json = r#"{
            "version": 1,
            "timestamp": "2026-04-26T12:00:00Z",
            "storePath": "/nix/store/abc"
        }"#;
        let root: GenerationRoot = serde_json::from_str(json).unwrap();
        assert_eq!(root.version, 1);
    }

    #[test]
    fn generation_root_deserializes_future_version() {
        let json = r#"{
            "version": 99,
            "timestamp": "2026-04-26T12:00:00Z",
            "storePath": "/nix/store/abc"
        }"#;
        let root: GenerationRoot = serde_json::from_str(json).unwrap();
        assert_eq!(root.version, 99);
        assert_eq!(root.store_path.as_str(), "/nix/store/abc");
    }

    #[test]
    fn generation_root_serializes_store_path_camel_case() {
        let root = GenerationRoot {
            version: 1,
            timestamp: Utc.with_ymd_and_hms(2026, 4, 26, 12, 0, 0).unwrap(),
            store_path: nsp("/nix/store/abc"),
        };
        let json = serde_json::to_string(&root).unwrap();
        assert!(json.contains("storePath"));
        assert!(!json.contains("store_path"));
    }
}
