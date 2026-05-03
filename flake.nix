{
  description = "Nix S3 Generations - GitHub Actions for Nix S3 binary cache with generations-based GC";

  nixConfig = {
    extra-substituters = [
      "https://nixcache.testquorum.dev"
    ];
    extra-trusted-public-keys = [
      "nixcache.testquorum.dev-1:aS+CJF8O8Ebirc6hypMfq/061h5TJlbsej1+zUJHPec="
    ];
  };

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    nix-fast-build = {
      url = "github:Mic92/nix-fast-build";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      crane,
      treefmt-nix,
      nix-fast-build,
    }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        pkgsStatic = pkgs.pkgsStatic;

        # --- treefmt ---
        treefmtEval = treefmt-nix.lib.evalModule pkgs {
          projectRootFile = "flake.nix";
          programs.rustfmt.enable = true;
          programs.prettier.enable = true;
          programs.nixfmt.enable = true;
          settings.global.excludes = [
            "dist/**"
            "gc/dist/**"
            "*.lock"
            "Cargo.lock"
            "package-lock.json"
            "LICENSE"
            ".envrc"
          ];
        };

        # --- Static build (for the release binary) ---
        craneLibStatic = crane.mkLib pkgsStatic;

        rustTargetSpec = pkgsStatic.stdenv.hostPlatform.rust.rustcTargetSpec;
        rustTargetSpecEnv = pkgsStatic.lib.toUpper (builtins.replaceStrings [ "-" ] [ "_" ] rustTargetSpec);

        staticArgs = {
          pname = "nix-s3-generations";
          version = "0.1.0";
          src = craneLibStatic.cleanCargoSource ./.;

          depsBuildBuild = with pkgsStatic; [
            buildPackages.stdenv.cc
            lld
          ];

          nativeBuildInputs = with pkgsStatic; [
            pkg-config
          ];

          buildInputs = with pkgsStatic; [
            openssl
          ];

          doIncludeCrossToolchainEnv = false;

          env.CARGO_BUILD_TARGET = rustTargetSpec;
          env."CARGO_TARGET_${rustTargetSpecEnv}_LINKER" = "${pkgsStatic.stdenv.cc.targetPrefix}cc";
        };

        staticCargoArtifacts = craneLibStatic.buildDepsOnly staticArgs;

        nix-s3-generations = craneLibStatic.buildPackage (
          staticArgs
          // {
            cargoArtifacts = staticCargoArtifacts;
          }
        );

        # --- Non-static crane (for checks) ---
        craneLib = crane.mkLib pkgs;
        checkSrc = craneLib.cleanCargoSource ./.;

        checkArgs = {
          pname = "nix-s3-generations";
          version = "0.1.0";
          src = checkSrc;
          nativeBuildInputs = with pkgs; [ pkg-config ];
          buildInputs = with pkgs; [ openssl ];
        };

        checkArtifacts = craneLib.buildDepsOnly checkArgs;

        # --- TypeScript checks ---
        npmFiles = [
          ./package.json
          ./package-lock.json
          ./tsconfig.json
        ];

        # Full action/ tree (incl. tests) for lint and test checks.
        tsSrc = pkgs.lib.fileset.toSource {
          root = ./.;
          fileset = pkgs.lib.fileset.unions (npmFiles ++ [ ./action ]);
        };

        # Per-bundle filesets exclude unrelated entrypoints and tests so that
        # touching gc-index.ts doesn't invalidate the main bundle and vice
        # versa.
        mainBundleSrc = pkgs.lib.fileset.toSource {
          root = ./.;
          fileset = pkgs.lib.fileset.unions (
            npmFiles
            ++ [
              ./action/index.ts
              ./action/binary-resolve.ts
              ./action/binary-download.ts
              ./action/helpers.ts
              ./action/state.ts
            ]
          );
        };

        gcBundleSrc = pkgs.lib.fileset.toSource {
          root = ./.;
          fileset = pkgs.lib.fileset.unions (
            npmFiles
            ++ [
              ./action/gc-index.ts
              ./action/binary-resolve.ts
              ./action/binary-download.ts
            ]
          );
        };

        npmDeps = pkgs.importNpmLock {
          npmRoot = ./.;
        };

        tsCheck =
          name: buildPhase:
          pkgs.buildNpmPackage {
            pname = "nix-s3-generations-${name}";
            version = "0.1.0";
            src = tsSrc;
            inherit npmDeps;
            npmConfigHook = pkgs.importNpmLock.npmConfigHook;
            dontNpmBuild = true;
            inherit buildPhase;
            installPhase = "touch $out";
          };

        bundle =
          {
            name,
            src,
            script,
            outDir,
          }:
          pkgs.buildNpmPackage {
            pname = "nix-s3-generations-dist-${name}";
            version = "0.1.0";
            inherit src;
            inherit npmDeps;
            npmConfigHook = pkgs.importNpmLock.npmConfigHook;
            dontNpmBuild = true;
            buildPhase = ''
              runHook preBuild
              npm run ${script}
              runHook postBuild
            '';
            installPhase = ''
              runHook preInstall
              mkdir -p $out
              cp -r ${outDir}/. $out/
              runHook postInstall
            '';
          };

        distMainBundle = bundle {
          name = "main";
          src = mainBundleSrc;
          script = "build:main";
          outDir = "dist";
        };

        distGcBundle = bundle {
          name = "gc";
          src = gcBundleSrc;
          script = "build:gc";
          outDir = "gc/dist";
        };

        # Verify the committed dist/ and gc/dist/ match what `npm run build`
        # would produce. One check covers both so that a stale bundle in
        # either location surfaces a single, consistent regeneration command.
        distUpToDate =
          pkgs.runCommand "nix-s3-generations-dist-up-to-date"
            {
              nativeBuildInputs = [ pkgs.diffutils ];
            }
            ''
              expected_main=${distMainBundle}
              expected_gc=${distGcBundle}
              actual_main=${./dist}
              actual_gc=${./gc/dist}

              fail=0
              if ! diff -r "$expected_main" "$actual_main" >/dev/null 2>&1; then
                echo "Diff in dist/:" >&2
                diff -r "$expected_main" "$actual_main" >&2 || true
                fail=1
              fi
              if ! diff -r "$expected_gc" "$actual_gc" >/dev/null 2>&1; then
                echo "Diff in gc/dist/:" >&2
                diff -r "$expected_gc" "$actual_gc" >&2 || true
                fail=1
              fi

              if [ "$fail" -ne 0 ]; then
                echo "" >&2
                echo "ERROR: dist/ and/or gc/dist/ is out of date relative to action/." >&2
                echo "To regenerate, run from the repo root:" >&2
                echo "  chmod -R u+w dist gc/dist 2>/dev/null" >&2
                echo "  rm -rf dist gc/dist" >&2
                echo "  cp -r $expected_main/. dist/" >&2
                echo "  cp -r $expected_gc/. gc/dist/" >&2
                exit 1
              fi
              touch $out
            '';
      in
      {
        packages = {
          default = nix-s3-generations;
          nix-fast-build = nix-fast-build.packages.${system}.default;
        };

        formatter = treefmtEval.config.build.wrapper;

        checks = {
          formatting = treefmtEval.config.build.check self;

          clippy = craneLib.cargoClippy (
            checkArgs
            // {
              cargoArtifacts = checkArtifacts;
              cargoClippyExtraArgs = "-- -D warnings";
            }
          );

          cargo-test = craneLib.cargoTest (
            checkArgs
            // {
              cargoArtifacts = checkArtifacts;
            }
          );

          ts-lint = tsCheck "ts-lint" "npm run lint";

          ts-test = tsCheck "ts-test" "npm test";

          dist-up-to-date = distUpToDate;

          # The static package build itself is also a check
          build = nix-s3-generations;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ nix-s3-generations ];

          packages = with pkgs; [
            rustc
            cargo
            clippy
            rustfmt
            nodejs
            shellcheck
          ];

          env = {
            RUST_SRC_PATH = "${pkgs.rustPlatform.rustcSrc}/library";
          };
        };
      }
    );
}
