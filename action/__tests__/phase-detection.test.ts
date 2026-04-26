import { describe, it, expect, vi, beforeEach } from "vitest";
import * as core from "@actions/core";
import { main, STATE_STARTED, STATE_ERROR_IN_MAIN } from "../index.js";

vi.mock("@actions/core");
vi.mock("@actions/exec");
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
}));

describe("phase detection", () => {
  let inputs: Record<string, string>;
  let stateStore: Map<string, string>;

  beforeEach(() => {
    inputs = {};
    stateStore = new Map();
    vi.clearAllMocks();
    process.env["AWS_ACCESS_KEY_ID"] = "test-akid";
    process.env["AWS_SECRET_ACCESS_KEY"] = "test-secret";

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

  describe("main routing", () => {
    it("routes to mainPhase when STATE_STARTED is empty", async () => {
      inputs = {
        "public-key":
          "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
        bucket: "my-bucket",
        "s3-endpoint": "abc.r2.cloudflarestorage.com",
        "private-key": "my-key",
        version: "test-sha",
      };
      stateStore.set(STATE_STARTED, "");

      await main();

      expect(core.saveState).toHaveBeenCalledWith(STATE_STARTED, "true");
    });

    it("routes to runPost when STATE_STARTED is 'true'", async () => {
      const { runPost } = await import("../helpers.js");
      inputs = {
        "public-key":
          "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
        bucket: "my-bucket",
        "s3-endpoint": "abc.r2.cloudflarestorage.com",
        "private-key": "my-key",
      };
      stateStore.set(STATE_STARTED, "true");

      await main();

      expect(runPost).toHaveBeenCalled();
    });
  });

  describe("error state propagation", () => {
    it("saves STATE_ERROR_IN_MAIN on unhandled error", async () => {
      inputs = {
        "public-key":
          "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
        bucket: "my-bucket",
        "s3-endpoint": "abc.r2.cloudflarestorage.com",
        "private-key": "my-key",
        version: "test-sha",
      };
      stateStore.set(STATE_STARTED, "");

      const { detectPlatform } = await import("../binary-download.js");
      vi.mocked(detectPlatform).mockImplementationOnce(() => {
        throw new Error("boom");
      });

      await main();

      expect(core.saveState).toHaveBeenCalledWith(STATE_ERROR_IN_MAIN, "true");
    });

    it("calls setFailed with error message on unhandled error", async () => {
      inputs = {
        "public-key":
          "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec=",
        bucket: "my-bucket",
        "s3-endpoint": "abc.r2.cloudflarestorage.com",
        "private-key": "my-key",
        version: "test-sha",
      };
      stateStore.set(STATE_STARTED, "");

      const { detectPlatform } = await import("../binary-download.js");
      vi.mocked(detectPlatform).mockImplementationOnce(() => {
        throw new Error("platform explosion");
      });

      await main();

      expect(core.setFailed).toHaveBeenCalledWith("platform explosion");
    });
  });
});
