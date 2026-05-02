import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import {
  detectPlatform,
  fetchArtifact,
  is404Error,
  unpackClosure,
} from "./binary-download.js";

export interface ResolveBinaryOptions {
  baseUrl: string;
  version: string;
}

// Bundle lives at `dist/index.js` for the main action and `gc/dist/index.js`
// for the gc action; the flake is always at the repo root above both.
function findFlakeRoot(): string {
  let dir = path.dirname(fileURLToPath(import.meta.url));
  for (let i = 0; i < 5; i++) {
    if (fs.existsSync(path.join(dir, "flake.nix"))) {
      return dir;
    }
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }
  throw new Error(
    `nix-s3-generations: could not find flake.nix walking up from ${path.dirname(fileURLToPath(import.meta.url))}`,
  );
}

async function buildBinaryFromCheckout(): Promise<string> {
  const root = findFlakeRoot();
  core.info(`Falling back to local build from ${root}`);
  let storePath = "";
  await exec.exec(
    "nix",
    ["build", "--no-link", "--no-write-lock-file", "--print-out-paths", root],
    {
      listeners: {
        stdout: (data: Buffer) => {
          storePath += data.toString();
        },
      },
    },
  );
  return path.join(storePath.trim(), "bin", "nix-s3-generations");
}

export async function resolveBinary(
  opts: ResolveBinaryOptions,
): Promise<string> {
  if (!opts.version) {
    return buildBinaryFromCheckout();
  }
  const platform = detectPlatform();
  core.info(`Detected platform: ${platform}`);
  try {
    const artifactPath = await fetchArtifact(
      platform,
      opts.version,
      opts.baseUrl,
    );
    return unpackClosure(artifactPath, "nix-s3-generations");
  } catch (error) {
    if (!is404Error(error)) {
      throw error;
    }
    return buildBinaryFromCheckout();
  }
}
