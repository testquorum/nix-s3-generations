import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { downloadTool } from "./helpers.js";

export function detectPlatform(): string {
  const arch = os.machine();
  const sys = os.type();

  if ((arch === "x86_64" || arch === "aarch64") && sys === "Linux") {
    return `${arch}-${sys}`;
  }

  throw new Error(
    `Unsupported platform: ${arch}-${sys}. Supported: x86_64-Linux, aarch64-Linux`,
  );
}

/**
 * Download a closure artifact from R2.
 *
 * Artifact naming convention: {baseUrl}/{version}/nix-s3-generations-closure-{platform}.closure.zst
 */
export async function fetchArtifact(
  platform: string,
  version: string,
  baseUrl: string,
): Promise<string> {
  const artifactName = `nix-s3-generations-closure-${platform}.closure.zst`;
  const base = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const url = `${base}${version}/${artifactName}`;
  core.info(`Downloading artifact from ${url}`);
  const downloadPath = await downloadTool(url);
  core.info(`Downloaded artifact to ${downloadPath}`);
  return downloadPath;
}

/**
 * Import a Nix closure archive and locate the binary within.
 *
 * Runs: cat <artifact> | zstd -d | nix-store --import
 * Then picks the imported path whose `bin/<binName>` exists. The binary's
 * package path is one of the paths the closure exports, so it must appear in
 * `nix-store --import`'s output.
 */
export async function unpackClosure(
  artifactPath: string,
  binName: string,
): Promise<string> {
  core.info(`Unpacking closure from ${artifactPath}`);

  // Pass `artifactPath` as a positional arg rather than interpolating it into
  // the shell string, so a path containing quotes/backticks/`$` can't be
  // re-interpreted by sh.
  let importOutput = "";
  await exec.exec(
    "sh",
    ["-c", 'cat -- "$1" | zstd -d | nix-store --import', "sh", artifactPath],
    {
      listeners: {
        stdout: (data: Buffer) => {
          importOutput += data.toString();
        },
      },
    },
  );

  const importedPaths = importOutput
    .split("\n")
    .map((p) => p.trim())
    .filter((p) => p.startsWith("/nix/store/"));

  core.info(`Imported ${importedPaths.length} store paths`);

  for (const storePath of importedPaths) {
    const binPath = path.join(storePath, "bin", binName);
    if (fs.existsSync(binPath)) {
      core.info(`Binary found at ${binPath}`);
      return binPath;
    }
  }

  throw new Error(
    `Binary '${binName}' not found among imported closure paths: ${importedPaths.join(", ")}`,
  );
}
