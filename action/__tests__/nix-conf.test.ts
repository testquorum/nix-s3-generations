import { describe, it, expect, vi } from "vitest";
import {
  buildAwsCredentialsFile,
  buildNixConf,
  buildNixS3Params,
  buildPostBuildHook,
  POST_BUILD_HOOK_PATH,
  SIGNING_KEY_PATH,
} from "../helpers.js";

vi.mock("@actions/core", () => ({
  info: vi.fn(),
  warning: vi.fn(),
  saveState: vi.fn(),
  getState: vi.fn(() => ""),
  getInput: vi.fn(() => ""),
  exportVariable: vi.fn(),
  setFailed: vi.fn(),
}));

describe("buildNixS3Params", () => {
  it("uses just region for native AWS S3", () => {
    expect(
      buildNixS3Params({
        bucket: "b",
        region: "us-east-1",
        endpoint: "s3.amazonaws.com",
        publicKey: "k",
      }),
    ).toBe("region=us-east-1");
  });

  it("uses just region for regional AWS S3 endpoints", () => {
    expect(
      buildNixS3Params({
        bucket: "b",
        region: "eu-west-1",
        endpoint: "s3.eu-west-1.amazonaws.com",
        publicKey: "k",
      }),
    ).toBe("region=eu-west-1");
  });

  it("includes endpoint for non-AWS S3-compatible providers", () => {
    expect(
      buildNixS3Params({
        bucket: "b",
        region: "auto",
        endpoint: "abc.r2.cloudflarestorage.com",
        publicKey: "k",
      }),
    ).toBe("endpoint=abc.r2.cloudflarestorage.com&region=auto");
  });
});

describe("buildNixConf", () => {
  it("emits substituter, trusted key, and post-build-hook", () => {
    const conf = buildNixConf({
      bucket: "my-bucket",
      region: "auto",
      endpoint: "abc.r2.cloudflarestorage.com",
      publicKey: "my-cache.example.com-1:ABC",
    });
    expect(conf).toContain(
      "extra-substituters = s3://my-bucket?endpoint=abc.r2.cloudflarestorage.com&region=auto",
    );
    expect(conf).toContain(
      "extra-trusted-public-keys = my-cache.example.com-1:ABC",
    );
    expect(conf).toContain(`post-build-hook = ${POST_BUILD_HOOK_PATH}`);
  });
});

describe("buildAwsCredentialsFile", () => {
  it("emits a [default] profile", () => {
    const content = buildAwsCredentialsFile({
      accessKeyId: "AKID",
      secretAccessKey: "SECRET",
      region: "auto",
    });
    expect(content).toContain("[default]");
    expect(content).toContain("aws_access_key_id = AKID");
    expect(content).toContain("aws_secret_access_key = SECRET");
    expect(content).toContain("region = auto");
  });
});

describe("buildPostBuildHook", () => {
  it("references the signing key, sets shell options, copies $OUT_PATHS", () => {
    const hook = buildPostBuildHook({
      bucket: "my-bucket",
      region: "auto",
      endpoint: "abc.r2.cloudflarestorage.com",
      publicKey: "k",
    });
    expect(hook).toMatch(/^#!\/bin\/bash/);
    expect(hook).toContain("set -eu");
    expect(hook).toContain("set -o pipefail");
    expect(hook).toContain(`secret-key=${SIGNING_KEY_PATH}`);
    expect(hook).toContain("compression=zstd");
    expect(hook).toContain("$OUT_PATHS");
    expect(hook).toContain(
      "s3://my-bucket?endpoint=abc.r2.cloudflarestorage.com&region=auto",
    );
  });
});
