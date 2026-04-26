import { describe, it, expect, vi, beforeEach } from "vitest";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as os from "node:os";
import {
  detectPlatform,
  fetchArtifact,
  unpackClosure,
} from "../binary-download.js";

vi.mock("@actions/core");
vi.mock("@actions/exec");
vi.mock("../helpers.js", () => ({
  downloadTool: vi.fn(async (url: string) => `/tmp/downloaded-${url.length}`),
}));
vi.mock("node:os");
vi.mock("node:fs");

describe("detectPlatform", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns x86_64-Linux on Linux x86_64", () => {
    vi.mocked(os.machine).mockReturnValue("x86_64");
    vi.mocked(os.type).mockReturnValue("Linux");
    expect(detectPlatform()).toBe("x86_64-Linux");
  });

  it("returns aarch64-Linux on Linux aarch64", () => {
    vi.mocked(os.machine).mockReturnValue("aarch64");
    vi.mocked(os.type).mockReturnValue("Linux");
    expect(detectPlatform()).toBe("aarch64-Linux");
  });

  it("rejects darwin", () => {
    vi.mocked(os.machine).mockReturnValue("arm64");
    vi.mocked(os.type).mockReturnValue("Darwin");
    expect(() => detectPlatform()).toThrow(/Unsupported platform/);
  });

  it("rejects windows", () => {
    vi.mocked(os.machine).mockReturnValue("x86_64");
    vi.mocked(os.type).mockReturnValue("Windows_NT");
    expect(() => detectPlatform()).toThrow(/Unsupported platform/);
  });

  it("rejects unsupported arch on Linux", () => {
    vi.mocked(os.machine).mockReturnValue("riscv64");
    vi.mocked(os.type).mockReturnValue("Linux");
    expect(() => detectPlatform()).toThrow(/Unsupported platform/);
  });
});

describe("fetchArtifact", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(core.info).mockImplementation(() => {});
  });

  it("constructs URL from baseUrl + version + platform-specific filename", async () => {
    const helpers = await import("../helpers.js");
    const downloadTool = vi.mocked(helpers.downloadTool);

    await fetchArtifact(
      "x86_64-Linux",
      "abc123",
      "https://assets.example.com/binaries/foo/",
    );
    expect(downloadTool).toHaveBeenCalledWith(
      "https://assets.example.com/binaries/foo/abc123/nix-s3-generations-closure-x86_64-Linux.closure.zst",
    );
  });

  it("appends a trailing slash to baseUrl when missing", async () => {
    const helpers = await import("../helpers.js");
    const downloadTool = vi.mocked(helpers.downloadTool);

    await fetchArtifact(
      "aarch64-Linux",
      "v1",
      "https://assets.example.com/binaries/foo",
    );
    expect(downloadTool).toHaveBeenCalledWith(
      "https://assets.example.com/binaries/foo/v1/nix-s3-generations-closure-aarch64-Linux.closure.zst",
    );
  });
});

describe("unpackClosure", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(core.info).mockImplementation(() => {});
  });

  it("returns path to binary inside an imported store path", async () => {
    vi.mocked(exec.exec).mockImplementation(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (async (_cmd: string, _args?: string[], opts?: any) => {
        opts?.listeners?.stdout?.(
          Buffer.from(
            "/nix/store/aaa-glibc\n/nix/store/bbb-nix-s3-generations\n",
          ),
        );
        return 0;
      }) as unknown as typeof exec.exec,
    );
    vi.mocked(fs.existsSync).mockImplementation((p) => {
      return p === "/nix/store/bbb-nix-s3-generations/bin/nix-s3-generations";
    });

    const result = await unpackClosure(
      "/tmp/closure.zst",
      "nix-s3-generations",
    );
    expect(result).toBe(
      "/nix/store/bbb-nix-s3-generations/bin/nix-s3-generations",
    );
  });

  it("passes the artifact path as a positional sh arg, not interpolated", async () => {
    vi.mocked(exec.exec).mockImplementation(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (async (_cmd: string, _args?: string[], opts?: any) => {
        opts?.listeners?.stdout?.(Buffer.from("/nix/store/x-pkg\n"));
        return 0;
      }) as unknown as typeof exec.exec,
    );
    vi.mocked(fs.existsSync).mockReturnValue(true);

    const tricky = '/tmp/has "quotes" and $vars/closure.zst';
    await unpackClosure(tricky, "nix-s3-generations");

    expect(exec.exec).toHaveBeenCalledWith(
      "sh",
      ["-c", 'cat -- "$1" | zstd -d | nix-store --import', "sh", tricky],
      expect.any(Object),
    );
  });

  it("throws when no imported path contains the named binary", async () => {
    vi.mocked(exec.exec).mockImplementation(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (async (_cmd: string, _args?: string[], opts?: any) => {
        opts?.listeners?.stdout?.(
          Buffer.from("/nix/store/aaa-glibc\n/nix/store/bbb-other-pkg\n"),
        );
        return 0;
      }) as unknown as typeof exec.exec,
    );
    vi.mocked(fs.existsSync).mockReturnValue(false);

    await expect(
      unpackClosure("/tmp/closure.zst", "nix-s3-generations"),
    ).rejects.toThrow(/not found among imported closure paths/);
  });

  it("ignores non-store-path lines in nix-store --import output", async () => {
    vi.mocked(exec.exec).mockImplementation(
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (async (_cmd: string, _args?: string[], opts?: any) => {
        opts?.listeners?.stdout?.(
          Buffer.from(
            "importing 1 paths\n/nix/store/aaa-pkg\n\nwarning: blah\n",
          ),
        );
        return 0;
      }) as unknown as typeof exec.exec,
    );
    vi.mocked(fs.existsSync).mockImplementation((p) => {
      return p === "/nix/store/aaa-pkg/bin/nix-s3-generations";
    });

    const result = await unpackClosure(
      "/tmp/closure.zst",
      "nix-s3-generations",
    );
    expect(result).toBe("/nix/store/aaa-pkg/bin/nix-s3-generations");
  });
});
