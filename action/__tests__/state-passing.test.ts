import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import * as core from "@actions/core";
import * as exec from "@actions/exec";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import {
  main,
  STATE_STARTED,
  STATE_STORE_SNAPSHOT,
  STATE_BIN_PATH,
} from "../index.js";

vi.mock("@actions/core");
vi.mock("@actions/exec", () => ({
  exec: vi.fn(async (_cmd: string, _args?: string[], options?: any) => {
    const data = Buffer.from("{}");
    options?.listeners?.stdout?.(data);
    return 0;
  }),
}));
vi.mock("../binary-download.js", () => ({
  detectPlatform: vi.fn(() => "x86_64-Linux"),
  fetchArtifact: vi.fn(async () => "/tmp/artifact.closure.zst"),
  unpackClosure: vi.fn(
    async () => "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
  ),
}));
vi.mock("../helpers.js", () => ({
  runPost: vi.fn(async () => {}),
  configureNixCache: vi.fn(async () => {}),
  makeTempDir: vi.fn((prefix: string) => {
    return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  }),
}));

describe("state passing", () => {
  let inputs: Record<string, string>;
  let stateStore: Map<string, string>;

  beforeEach(() => {
    inputs = {};
    stateStore = new Map();
    vi.clearAllMocks();
    process.env["AWS_ACCESS_KEY_ID"] = "test-akid";
    process.env["AWS_SECRET_ACCESS_KEY"] = "test-secret";
    process.env["GITHUB_ACTION_REF"] = "test-sha";

    vi.mocked(core.getInput).mockImplementation(
      (name: string) => inputs[name] ?? "",
    );
    vi.mocked(core.getState).mockImplementation(
      (name: string) => stateStore.get(name) ?? "",
    );
    vi.mocked(core.saveState).mockImplementation(
      (name: string, value: string) => {
        stateStore.set(name, value);
      },
    );
    vi.mocked(core.exportVariable).mockImplementation(() => {});
    vi.mocked(core.info).mockImplementation(() => {});
    vi.mocked(core.warning).mockImplementation(() => {});
    vi.mocked(core.setFailed).mockImplementation(() => {});
  });

  afterEach(() => {
    delete process.env["GITHUB_ACTION_REF"];
  });

  it("saves STATE_STARTED as 'true' after mainPhase completes", async () => {
    inputs = {
      "public-key":
        "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
      bucket: "my-bucket",
      "s3-endpoint": "abc.r2.cloudflarestorage.com",
      "private-key": "my-key",
    };
    stateStore.set(STATE_STARTED, "");

    await main();

    expect(stateStore.get(STATE_STARTED)).toBe("true");
  });

  it("saves store snapshot via saveState during mainPhase", async () => {
    inputs = {
      "public-key":
        "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
      bucket: "my-bucket",
      "s3-endpoint": "abc.r2.cloudflarestorage.com",
      "private-key": "my-key",
    };
    stateStore.set(STATE_STARTED, "");

    await main();

    expect(core.saveState).toHaveBeenCalledWith(
      STATE_STORE_SNAPSHOT,
      expect.stringContaining("store-snapshot.json"),
    );
  });

  it("saves bin path via saveState during mainPhase", async () => {
    inputs = {
      "public-key":
        "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
      bucket: "my-bucket",
      "s3-endpoint": "abc.r2.cloudflarestorage.com",
      "private-key": "my-key",
    };
    stateStore.set(STATE_STARTED, "");

    await main();

    expect(core.saveState).toHaveBeenCalledWith(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );
  });

  it("simulates state round-trip: main saves, post reads", async () => {
    const { runPost } = await import("../helpers.js");
    inputs = {
      "public-key":
        "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
      bucket: "my-bucket",
      "s3-endpoint": "abc.r2.cloudflarestorage.com",
      "private-key": "my-key",
      region: "us-west-2",
    };

    stateStore.set(STATE_STARTED, "");

    await main();

    expect(stateStore.get(STATE_STARTED)).toBe("true");
    const savedSnapshot = stateStore.get(STATE_STORE_SNAPSHOT);
    expect(savedSnapshot).toBeDefined();
    expect(savedSnapshot).toContain("store-snapshot.json");

    stateStore.set(STATE_STARTED, "true");
    stateStore.set(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );

    vi.mocked(core.getInput).mockImplementation(
      (name: string) => inputs[name] ?? "",
    );

    await main();

    expect(runPost).toHaveBeenCalled();
  });

  it("fails when AWS credentials are missing", async () => {
    delete process.env["AWS_ACCESS_KEY_ID"];
    delete process.env["AWS_SECRET_ACCESS_KEY"];
    inputs = {
      "public-key": "k",
      bucket: "my-bucket",
      "s3-endpoint": "abc.r2.cloudflarestorage.com",
      "private-key": "my-key",
    };
    stateStore.set(STATE_STARTED, "");

    await main();

    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining("AWS credentials"),
    );
  });

  it("runPost fails with descriptive error when snapshot file is missing", async () => {
    const { runPost } = await vi.importActual<{ runPost: () => Promise<void> }>(
      "../helpers.js",
    );
    stateStore.set(
      STATE_STORE_SNAPSHOT,
      "/tmp/nonexistent-snapshot-12345/store-snapshot.json",
    );
    stateStore.set(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );

    await runPost();

    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining("snapshot file not found"),
    );
  });

  it("runPost fails when snapshot state is empty", async () => {
    const { runPost } = await vi.importActual<{ runPost: () => Promise<void> }>(
      "../helpers.js",
    );
    stateStore.set(STATE_STORE_SNAPSHOT, "");
    stateStore.set(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );

    await runPost();

    expect(core.setFailed).toHaveBeenCalledWith(
      expect.stringContaining("snapshot path not found"),
    );
  });

  it("runPost cleans up snapshot dir on success", async () => {
    const { runPost } = await vi.importActual<{ runPost: () => Promise<void> }>(
      "../helpers.js",
    );
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "test-snapshot-"));
    const snapshotPath = path.join(tmpDir, "store-snapshot.json");
    fs.writeFileSync(snapshotPath, "{}");

    stateStore.set(STATE_STORE_SNAPSHOT, snapshotPath);
    stateStore.set(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );

    vi.mocked(exec.exec).mockResolvedValue(0);

    await runPost();

    expect(fs.existsSync(tmpDir)).toBe(false);
  });

  it("runPost cleans up snapshot dir on binary failure", async () => {
    const { runPost } = await vi.importActual<{ runPost: () => Promise<void> }>(
      "../helpers.js",
    );
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "test-snapshot-"));
    const snapshotPath = path.join(tmpDir, "store-snapshot.json");
    fs.writeFileSync(snapshotPath, "{}");

    stateStore.set(STATE_STORE_SNAPSHOT, snapshotPath);
    stateStore.set(
      STATE_BIN_PATH,
      "/nix/store/xxx-nix-s3-generations/bin/nix-s3-generations",
    );

    vi.mocked(exec.exec).mockResolvedValue(1);

    await runPost();

    expect(fs.existsSync(tmpDir)).toBe(false);
  });
});
