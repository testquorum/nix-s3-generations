import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  STATE_BIN_PATH,
  STATE_ERROR_IN_MAIN,
  STATE_STORE_SNAPSHOT,
} from "./state.js";

export const NIX_CONF_PATH = "/etc/nix/nix.conf";
export const AWS_CREDENTIALS_PATH = "/root/.aws/credentials";
export const SIGNING_KEY_PATH = "/etc/nix/cache-priv-key.pem";
// The path is unique to this action so the Rust binary can detect *our*
// hook (not just any post-build-hook). Must stay in sync with
// `OUR_POST_BUILD_HOOK_PATH` in src/nix.rs.
export const POST_BUILD_HOOK_PATH =
  "/etc/nix/nix-s3-generations-post-build-hook.sh";

export interface NixCacheConfig {
  bucket: string;
  region: string;
  endpoint: string;
  publicKey: string;
}

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
}

/// Substituter URL parameters: encoded as `&`-separated key=value pairs that
/// follow the bucket name in `s3://{bucket}?...`. Native AWS S3 doesn't need
/// `endpoint=`; everything else does.
export function buildNixS3Params(cfg: NixCacheConfig): string {
  const isNativeAws =
    cfg.endpoint === "s3.amazonaws.com" ||
    /^s3\.[^.]+\.amazonaws\.com$/.test(cfg.endpoint);
  if (isNativeAws) {
    return `region=${cfg.region}`;
  }
  return `endpoint=${cfg.endpoint}&region=${cfg.region}`;
}

/// Snippet appended to nix.conf. Leading newline so we don't accidentally
/// join to a non-newline-terminated last line of the existing file.
export function buildNixConf(cfg: NixCacheConfig): string {
  const s3Params = buildNixS3Params(cfg);
  return [
    "",
    "# nix-s3-generations",
    `extra-substituters = s3://${cfg.bucket}?${s3Params}`,
    `extra-trusted-public-keys = ${cfg.publicKey}`,
    `post-build-hook = ${POST_BUILD_HOOK_PATH}`,
    "",
  ].join("\n");
}

export function buildAwsCredentialsFile(creds: AwsCredentials): string {
  return [
    "[default]",
    `aws_access_key_id = ${creds.accessKeyId}`,
    `aws_secret_access_key = ${creds.secretAccessKey}`,
    `region = ${creds.region}`,
    "",
  ].join("\n");
}

/// Post-build hook script. Runs as root via the Nix daemon after every
/// successful build; signs and uploads each output path immediately.
/// AWS credentials are picked up from /root/.aws/credentials by the SDK.
export function buildPostBuildHook(cfg: NixCacheConfig): string {
  const s3Params = buildNixS3Params(cfg);
  const copyUri = `s3://${cfg.bucket}?${s3Params}&secret-key=${SIGNING_KEY_PATH}&compression=zstd`;
  return [
    "#!/bin/bash",
    "set -eu",
    "set -o pipefail",
    "",
    'echo "nix-s3-generations: uploading $OUT_PATHS"',
    `exec /nix/var/nix/profiles/default/bin/nix copy --to "${copyUri}" $OUT_PATHS`,
    "",
  ].join("\n");
}

async function sudoWriteFile(
  destPath: string,
  content: string,
  mode: string,
): Promise<void> {
  const parent = path.dirname(destPath);
  await exec.exec("sudo", ["mkdir", "-p", parent]);
  await exec.exec("sudo", ["tee", destPath], {
    input: Buffer.from(content),
    silent: true,
  });
  await exec.exec("sudo", ["chmod", mode, destPath]);
}

/// Append to a file already owned by another component (e.g. the Nix installer
/// owns /etc/nix/nix.conf and may have set experimental-features). Don't
/// chmod — leave the existing perms alone.
async function sudoAppendFile(
  destPath: string,
  content: string,
): Promise<void> {
  await exec.exec("sudo", ["tee", "-a", destPath], {
    input: Buffer.from(content),
    silent: true,
  });
}

export async function configureNixCache(
  cfg: NixCacheConfig,
  creds: AwsCredentials,
  signingKey: string,
): Promise<void> {
  await sudoWriteFile(
    AWS_CREDENTIALS_PATH,
    buildAwsCredentialsFile(creds),
    "0600",
  );
  core.info(`Wrote AWS credentials to ${AWS_CREDENTIALS_PATH}`);

  await sudoWriteFile(SIGNING_KEY_PATH, signingKey, "0600");
  core.info(`Wrote signing key to ${SIGNING_KEY_PATH}`);

  await sudoWriteFile(POST_BUILD_HOOK_PATH, buildPostBuildHook(cfg), "0755");
  core.info(`Wrote post-build hook to ${POST_BUILD_HOOK_PATH}`);

  // Append to /etc/nix/nix.conf so we don't clobber settings the Nix installer
  // already wrote (e.g. `experimental-features = nix-command flakes`).
  await sudoAppendFile(NIX_CONF_PATH, buildNixConf(cfg));
  core.info(`Appended cache settings to ${NIX_CONF_PATH}`);

  // Restart the daemon so it picks up the new substituter, trusted key,
  // and post-build-hook. Tolerate failure on systems without systemd.
  await exec.exec("sudo", ["systemctl", "restart", "nix-daemon"], {
    ignoreReturnCode: true,
  });
}

export async function runPost(): Promise<void> {
  core.info("nix-s3-generations: post phase");

  const errorInMain = core.getState(STATE_ERROR_IN_MAIN);
  if (errorInMain === "true") {
    core.info("nix-s3-generations: main phase had an error, skipping post");
    return;
  }

  const preSnapshotPath = core.getState(STATE_STORE_SNAPSHOT);
  const binPath = core.getState(STATE_BIN_PATH);

  if (!preSnapshotPath) {
    core.setFailed("nix-s3-generations: snapshot path not found in state");
    return;
  }

  if (!fs.existsSync(preSnapshotPath)) {
    core.setFailed(
      `nix-s3-generations: snapshot file not found at ${preSnapshotPath}`,
    );
    return;
  }

  core.info(`Using snapshot file: ${preSnapshotPath}`);

  if (!binPath) {
    core.setFailed(
      "nix-s3-generations: binary path not found in state, cannot run push",
    );
    return;
  }

  const bucket = core.getInput("bucket", { required: true });
  const region = core.getInput("region") || "us-east-1";
  const endpoint = core.getInput("s3-endpoint", { required: true });

  // The binary runs as the runner user — it can't read /root/.aws/credentials,
  // and inputs aren't visible to the AWS SDK. Forward the creds explicitly so
  // both `nix copy` and the in-process S3 client can authenticate.
  const accessKeyId =
    core.getInput("aws-access-key-id") ||
    process.env["AWS_ACCESS_KEY_ID"] ||
    "";
  const secretAccessKey =
    core.getInput("aws-secret-access-key") ||
    process.env["AWS_SECRET_ACCESS_KEY"] ||
    "";

  // The GC domain is the {repo}/{workflow} pair: generations are numbered
  // and aged out independently per (repo, workflow). The numeric generation
  // is the run number — monotonic per workflow run. Matrix legs share the
  // run number and write distinct shard files under it; re-runs reuse the
  // same run number and intentionally overwrite their shards.
  const domain = [
    process.env["GITHUB_REPOSITORY"] ?? "",
    process.env["GITHUB_WORKFLOW"] ?? "",
  ].join("/");
  const generation = process.env["GITHUB_RUN_NUMBER"] ?? "";

  const args: string[] = [
    "push",
    "--bucket",
    bucket,
    "--region",
    region,
    "--endpoint",
    endpoint,
    "--snapshot-before",
    preSnapshotPath,
    "--domain",
    domain,
    "--generation",
    generation,
  ];

  // Only pass --shard-id when RUNNER_NAME is non-empty; the binary defaults
  // to "gen" otherwise. Empty would fail validation in the binary.
  const runnerName = process.env["RUNNER_NAME"];
  if (runnerName) {
    args.push("--shard-id", runnerName);
  }

  core.info(`Running push with args: ${args.join(" ")}`);

  const childEnv: Record<string, string> = {
    ...(process.env as Record<string, string>),
    AWS_ACCESS_KEY_ID: accessKeyId,
    AWS_SECRET_ACCESS_KEY: secretAccessKey,
    AWS_REGION: region,
    AWS_DEFAULT_REGION: region,
  };

  let exitCode = 0;
  try {
    exitCode = await exec.exec(binPath, args, {
      ignoreReturnCode: true,
      env: childEnv,
    });
  } catch (error) {
    core.setFailed(
      `nix-s3-generations: failed to execute push binary: ${error instanceof Error ? error.message : String(error)}`,
    );
    cleanupTempFiles(path.dirname(preSnapshotPath));
    return;
  }

  if (exitCode === 0) {
    core.info("nix-s3-generations: push completed successfully");
  } else {
    core.setFailed(
      `nix-s3-generations: push failed with exit code ${exitCode}`,
    );
  }

  cleanupTempFiles(path.dirname(preSnapshotPath));

  core.info("nix-s3-generations: post phase complete");
}

function cleanupTempFiles(tmpDir: string): void {
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    core.info(`Cleaned up temp dir: ${tmpDir}`);
  } catch {
    core.warning(`Failed to clean up temp dir: ${tmpDir}`);
  }
}

export function getTempDir(): string {
  const tmpDir = process.env["RUNNER_TEMP"] || os.tmpdir();
  return tmpDir;
}

export function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(getTempDir(), prefix));
}
