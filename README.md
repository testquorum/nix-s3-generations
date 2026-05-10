# nix-s3-generations

> [!CAUTION]
> While the push format of `nix-s3-generations` has not changed since its design, the garbage collection side still has risks.
>
> Using the push side of this action will allow future garbage collection to work successfully, but it is not recommended to enable garbage collection as of yet.

Push Nix closures to an S3-backed binary cache with generation-based GC roots, then prune unreachable objects on a schedule.

## S3 Bucket Layout

```
<bucket>/
│
├── nix-cache-info                        ← Nix substituter discovery (created manually)
│
├── <hash>.narinfo                        ← One per store path in the cache
│   │                                       e.g. x4ayiscwbhcj89ija7s294jrdjss4009.narinfo
│   │
│   └── Fields:
│       StorePath: /nix/store/<hash>-<name>
│       URL:        nar/<hash>.nar.<ext>    ← points into nar/
│       References: <hash>-<name> ...       ← other store paths in the closure
│       …                                    (NarHash, NarSize, Sig, etc.)
│
├── nar/
│   ├── <hash>.nar                        ← Uncompressed NAR
│   ├── <hash>.nar.xz                     ← xz-compressed NAR
│   ├── <hash>.nar.zst                     ← zstd-compressed NAR
│   └── …                                  One file per unique store path content
│                                            Content-addressed by the NAR hash,
│                                            not the store-path hash.
│
└── generations/                           ← GC roots (one JSON per shard)
    └── <domain>/                          ← Arbitrary namespace (e.g. owner/repo/ci)
        └── <generation>/                 ← Monotonically increasing number per domain
            ├── <shard-id>.json           ← Generation root (default shard: "gen")
            └── …                          Multiple shards per generation are allowed
```

### Key formats

| S3 key          | Format                                              | Example                                                           |
| --------------- | --------------------------------------------------- | ----------------------------------------------------------------- |
| Generation root | `generations/{domain}/{generation}/{shard-id}.json` | `generations/owner/repo/ci/42/GitHub Actions 3.json`              |
| NAR info        | `{hash}.narinfo`                                    | `x4ayiscwbhcj89ija7s294jrdjss4009.narinfo`                        |
| NAR             | `nar/{hash}.nar[.<ext>]`                            | `nar/0wab6kssgzmc47dx8qnc9mq2xbgvdbjwv3500yxsxkf68faaan9y.nar.xz` |

### Generation root JSON

Each generation root file is a JSON document:

```json
{
  "version": 1,
  "timestamp": "2026-05-10T12:00:00Z",
  "storePath": "/nix/store/x4ay…-nix-s3-generations-closure-root"
}
```

The `storePath` is a "closure root" — a single Nix derivation whose runtime references are exactly the minimal cover of paths pushed in that generation. Walking its transitive closure via `.narinfo` references finds every reachable path.

### How it connects

```
  push ──► generations/ ◄── forget
               │                  │
               ▼                  ▼
         .narinfo files      delete generation JSONs
               │
               ▼
            nar/ ◄────────── prune (nar subcommand)
               │                  │
               ▼                  ▼
         NAR blobs           delete orphan NARs
```

1. **push** — Snapshots the Nix store, builds a closure root, and relies on a post-build hook to upload NARs + narinfos. Writes a generation root JSON to `generations/{domain}/{generation}/{shard-id}.json`.

2. **forget** — Enumerates generation roots under `generations/`, applies retention policies (`--keep-last`, `--keep-within`, `--forget-before`), and deletes generation JSONs that fall outside the policy. Optionally chains into `prune`.

3. **prune** — Two-phase mark-and-sweep:
   - **narinfo phase**: Walks every generation root → DFS through `.narinfo` references → builds the set of "live" store-path hashes. Deletes any `.narinfo` whose hash isn't live.
   - **NAR phase**: Scans every surviving `.narinfo` for `URL:` fields to build a set of referenced `nar/` keys. Deletes any `nar/<hash>.nar[.<ext>]` not in that set.

Both phases skip objects whose `LastModified` is within `--freshness-buffer-secs` of prune start, protecting in-flight pushes.
