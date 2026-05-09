//! Coordination types for the prune ↔ push race protocol.
//!
//! See `meta/prune-attempts/` in S3: the prune publishes a `PruneAttempt`
//! marker before any deletes, and pushers whose closure overlaps with an
//! attempt's `delete_set` upload `PushConflict` files so the prune can
//! subtract those hashes and yield the current attempt to the pusher.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// State of a single prune attempt. The same prune CLI invocation may
/// publish multiple sequential attempts: each time a conflict is honoured,
/// the current attempt flips to `Done` and a fresh attempt is published
/// with the conflicted hashes removed from its `delete_set`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AttemptStatus {
    Running,
    Done,
}

/// On-disk JSON for `meta/prune-attempts/{attempt_id}.json`.
///
/// Mutable while `status == Running`: the file is overwritten when the
/// attempt finishes (status flips to `Done`, `ended_at` is set). After
/// `Done`, callers must treat the file as immutable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PruneAttempt {
    pub version: u32,
    pub attempt_id: String,
    pub started_at: DateTime<Utc>,
    /// Set when `status` flips to `Done`. `None` while running.
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
    pub status: AttemptStatus,
    /// Hashes (the 32-character base32 prefix part of a narinfo key, no
    /// `.narinfo` suffix) that this attempt is committed to deleting.
    /// Pushers intersect their closure-hash claim with this set to detect
    /// conflicts.
    pub delete_set: Vec<String>,
}

impl PruneAttempt {
    pub fn new_running(attempt_id: String, delete_set: Vec<String>) -> Self {
        Self {
            version: 1,
            attempt_id,
            started_at: Utc::now(),
            ended_at: None,
            status: AttemptStatus::Running,
            delete_set,
        }
    }

    pub fn finalize(&mut self) {
        self.status = AttemptStatus::Done;
        self.ended_at = Some(Utc::now());
    }
}

/// On-disk JSON for `meta/prune-attempts/{attempt_id}/conflicts/{push_id}.json`.
///
/// Uploaded by a push when its closure-hash claim overlaps with an attempt's
/// `delete_set`. The attempt loops between batches reading every conflict
/// under its prefix; if any conflict's `claim` intersects the remaining
/// delete set, those hashes are removed and the current attempt is finalized
/// before the next batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushConflict {
    pub version: u32,
    pub push_id: String,
    pub uploaded_at: DateTime<Utc>,
    /// Closure-hash set the pusher needs preserved. Same hash format as
    /// `PruneAttempt::delete_set`.
    pub claim: Vec<String>,
}

impl PushConflict {
    pub fn new(push_id: String, claim: Vec<String>) -> Self {
        Self {
            version: 1,
            push_id,
            uploaded_at: Utc::now(),
            claim,
        }
    }
}

/// Generate a fresh attempt or push identifier.
///
/// Format: `YYYYMMDDTHHMMSS.nnnnnnnnnZ-{pid:08x}`. Lex-sortable by start
/// time, distinguishes concurrent runs on the same machine via PID. The
/// nanosecond resolution makes within-process collisions effectively
/// impossible without pulling in a UUID dependency for what is otherwise
/// a single small string.
pub fn new_id() -> String {
    let now = Utc::now();
    format!(
        "{}-{:08x}",
        now.format("%Y%m%dT%H%M%S%.9fZ"),
        std::process::id()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn attempt_round_trip() {
        let mut a = PruneAttempt::new_running(
            "20260509T120000.000000000Z-deadbeef".to_string(),
            vec!["aaaa".to_string(), "bbbb".to_string()],
        );
        a.started_at = Utc.with_ymd_and_hms(2026, 5, 9, 12, 0, 0).unwrap();

        let json = serde_json::to_string(&a).unwrap();
        let back: PruneAttempt = serde_json::from_str(&json).unwrap();
        assert_eq!(back.attempt_id, a.attempt_id);
        assert_eq!(back.delete_set, a.delete_set);
        assert_eq!(back.status, AttemptStatus::Running);
        assert!(back.ended_at.is_none());
    }

    #[test]
    fn attempt_status_serializes_lowercase() {
        let json = serde_json::to_string(&AttemptStatus::Running).unwrap();
        assert_eq!(json, "\"running\"");
        let json = serde_json::to_string(&AttemptStatus::Done).unwrap();
        assert_eq!(json, "\"done\"");
    }

    #[test]
    fn finalize_sets_status_and_ended_at() {
        let mut a = PruneAttempt::new_running("id".to_string(), vec![]);
        assert_eq!(a.status, AttemptStatus::Running);
        assert!(a.ended_at.is_none());
        a.finalize();
        assert_eq!(a.status, AttemptStatus::Done);
        assert!(a.ended_at.is_some());
    }

    #[test]
    fn conflict_round_trip() {
        let c = PushConflict::new(
            "push-1".to_string(),
            vec!["aaaa".to_string(), "cccc".to_string()],
        );
        let json = serde_json::to_string(&c).unwrap();
        let back: PushConflict = serde_json::from_str(&json).unwrap();
        assert_eq!(back.push_id, "push-1");
        assert_eq!(back.claim, vec!["aaaa".to_string(), "cccc".to_string()]);
    }

    #[test]
    fn ids_are_unique_and_sortable() {
        let a = new_id();
        // Sleep briefly to advance the nanosecond timestamp.
        std::thread::sleep(std::time::Duration::from_millis(1));
        let b = new_id();
        assert_ne!(a, b);
        assert!(a < b, "ids should sort by time: {a} vs {b}");
    }
}
