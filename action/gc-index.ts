import * as core from "@actions/core";
import * as exec from "@actions/exec";
import { resolveBinary } from "./binary-resolve.js";

const DEFAULT_BASE_URL =
  "https://assets.testquorum.dev/binaries/nix-s3-generations/";
const DEFAULT_REGION = "us-east-1";

function resolveCredential(inputName: string, envName: string): string {
  return core.getInput(inputName) || process.env[envName] || "";
}

async function gcPhase(): Promise<void> {
  const bucket = core.getInput("bucket", { required: true });
  const endpoint = core.getInput("s3-endpoint", { required: true });
  const region = core.getInput("region") || DEFAULT_REGION;
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
      "nix-s3-generations gc: AWS credentials not provided. Set aws-access-key-id and aws-secret-access-key inputs, or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env vars.",
    );
  }

  const binPath = await resolveBinary({ baseUrl, version });
  core.info(`Binary available at: ${binPath}`);

  const args: string[] = [
    "forget",
    "--bucket",
    bucket,
    "--region",
    region,
    "--endpoint",
    endpoint,
    "--prune",
  ];

  for (const d of core.getMultilineInput("domain")) {
    args.push("--domain", d);
  }

  const keepLast = core.getInput("keep-last");
  if (keepLast) args.push("--keep-last", keepLast);

  const keepWithin = core.getInput("keep-within");
  if (keepWithin) args.push("--keep-within", keepWithin);

  const forgetBefore = core.getInput("forget-before");
  if (forgetBefore) args.push("--forget-before", forgetBefore);

  const freshnessBufferSecs = core.getInput("freshness-buffer-secs");
  if (freshnessBufferSecs)
    args.push("--freshness-buffer-secs", freshnessBufferSecs);

  core.info(`Running forget with args: ${args.join(" ")}`);

  const childEnv: Record<string, string> = {
    ...(process.env as Record<string, string>),
    AWS_ACCESS_KEY_ID: accessKeyId,
    AWS_SECRET_ACCESS_KEY: secretAccessKey,
    AWS_REGION: region,
    AWS_DEFAULT_REGION: region,
  };

  const exitCode = await exec.exec(binPath, args, {
    ignoreReturnCode: true,
    env: childEnv,
  });
  if (exitCode !== 0) {
    core.setFailed(
      `nix-s3-generations gc: forget failed with exit code ${exitCode}`,
    );
  }
}

async function main(): Promise<void> {
  try {
    await gcPhase();
  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(error.message);
    } else {
      core.setFailed(String(error));
    }
  }
}

main();
