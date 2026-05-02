import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as path from "node:path";
import { resolveBinary } from "./binary-resolve.js";
import { configureNixCache, makeTempDir, runPost } from "./helpers.js";
import {
  STATE_BIN_PATH,
  STATE_ERROR_IN_MAIN,
  STATE_STARTED,
  STATE_STORE_SNAPSHOT,
} from "./state.js";

export {
  STATE_BIN_PATH,
  STATE_ERROR_IN_MAIN,
  STATE_STARTED,
  STATE_STORE_SNAPSHOT,
} from "./state.js";

let writeStream: fs.WriteStream | undefined;

const DEFAULT_BASE_URL =
  "https://assets.testquorum.dev/binaries/nix-s3-generations/";
const DEFAULT_REGION = "us-east-1";

function resolveCredential(inputName: string, envName: string): string {
  return core.getInput(inputName) || process.env[envName] || "";
}

export async function mainPhase(): Promise<void> {
  const bucket = core.getInput("bucket", { required: true });
  const region = core.getInput("region") || DEFAULT_REGION;
  const endpoint = core.getInput("s3-endpoint", { required: true });
  const publicKey = core.getInput("public-key", { required: true });
  const signingKey = core.getInput("private-key", { required: true });
  const baseUrl = core.getInput("binary-base-url") || DEFAULT_BASE_URL;
  const version = process.env["GITHUB_ACTION_REF"] || "";

  const accessKeyId = resolveCredential(
    "aws-access-key-id",
    "AWS_ACCESS_KEY_ID",
  );
  const secretAccessKey = resolveCredential(
    "aws-secret-access-key",
    "AWS_SECRET_ACCESS_KEY",
  );
  if (!accessKeyId || !secretAccessKey) {
    throw new Error(
      "nix-s3-generations: AWS credentials not provided. Set aws-access-key-id and aws-secret-access-key inputs, or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars.",
    );
  }

  // Configure the cache before resolving the binary so the fallback build
  // benefits from the substituter and its outputs hit the post-build hook.
  await configureNixCache(
    { bucket, region, endpoint, publicKey },
    {
      accessKeyId,
      secretAccessKey,
      region,
    },
    signingKey,
  );

  const binPath = await resolveBinary({ baseUrl, version });
  core.info(`Binary available at: ${binPath}`);
  core.saveState(STATE_BIN_PATH, binPath);

  // Pin `--json-format 1` so the output shape is the same `{path: meta}` map
  // regardless of the installed Nix version's default. Some Nix versions
  // default to format 2 which wraps entries under `paths`, which the binary's
  // parser would treat as an empty snapshot. Failures here propagate to the
  // outer catch in `main()`, which marks the action failed; the post phase
  // then sees STATE_ERROR_IN_MAIN and skips cleanly. We intentionally don't
  // fall back to "{}" — that would make every existing path look "new" in
  // post and trigger an unrelated mass push.
  const snapshotDir = makeTempDir("nix-s3-generations-snapshot-");
  const snapshotPath = path.join(snapshotDir, "store-snapshot.json");
  writeStream = fs.createWriteStream(snapshotPath);
  writeStream.on("error", (err) => {
    core.warning(`Failed to write snapshot: ${err.message}`);
  });
  await exec.exec(
    "nix",
    ["path-info", "--json", "--json-format", "1", "--all"],
    {
      silent: true,
      listeners: {
        stdout: (data: Buffer) => {
          writeStream!.write(data);
        },
      },
    },
  );
  writeStream.end();
  await new Promise<void>((resolve, reject) => {
    writeStream!.on("finish", resolve);
    writeStream!.on("error", reject);
  });
  core.saveState(STATE_STORE_SNAPSHOT, snapshotPath);
  core.info(`Wrote store snapshot to ${snapshotPath}`);

  core.saveState(STATE_STARTED, "true");
}

export async function main(): Promise<void> {
  try {
    // Route on either flag: STATE_STARTED is the success signal, but on a
    // mainPhase failure we still want post to take the runPost branch (which
    // short-circuits on STATE_ERROR_IN_MAIN) instead of re-running mainPhase.
    const started = core.getState(STATE_STARTED);
    const errored = core.getState(STATE_ERROR_IN_MAIN);
    if (started === "" && errored === "") {
      await mainPhase();
    } else {
      await runPost();
    }
  } catch (error) {
    if (typeof writeStream !== "undefined") {
      writeStream.destroy();
    }
    core.saveState(STATE_ERROR_IN_MAIN, "true");
    if (error instanceof Error) {
      core.setFailed(error.message);
    } else {
      core.setFailed(String(error));
    }
  }
}

main();
