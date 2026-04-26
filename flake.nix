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
        tsSrc = pkgs.lib.fileset.toSource {
          root = ./.;
          fileset = pkgs.lib.fileset.unions [
            ./package.json
            ./package-lock.json
            ./tsconfig.json
            ./action
          ];
        };

        npmDepsHash = "sha256-m0QtbzuwfZckjbki9jwgBRGkJv93IrJ5/ERDlRRJRhs=";

        tsCheck =
          name: buildPhase:
          pkgs.buildNpmPackage {
            pname = "nix-s3-generations-${name}";
            version = "0.1.0";
            src = tsSrc;
            inherit npmDepsHash;
            dontNpmBuild = true;
            inherit buildPhase;
            installPhase = "touch $out";
          };

        # The action's compiled bundle (what gets shipped at dist/index.js).
        # Built reproducibly from src/ via ncc.
        distBundle = pkgs.buildNpmPackage {
          pname = "nix-s3-generations-dist";
          version = "0.1.0";
          src = tsSrc;
          inherit npmDepsHash;
          dontNpmBuild = true;
          buildPhase = ''
            runHook preBuild
            npm run build
            runHook postBuild
          '';
          installPhase = ''
            runHook preInstall
            mkdir -p $out
            cp -r dist/. $out/
            runHook postInstall
          '';
        };

        # Verify the committed dist/ matches what `npm run build` would produce.
        distUpToDate =
          pkgs.runCommand "nix-s3-generations-dist-up-to-date"
            {
              nativeBuildInputs = [ pkgs.diffutils ];
            }
            ''
              expected=${distBundle}
              actual=${./dist}
              if ! diff -r "$expected" "$actual" >/dev/null 2>&1; then
                echo "Diff between expected (Nix-built) and actual (committed) dist/:" >&2
                diff -r "$expected" "$actual" >&2 || true
                echo "" >&2
                echo "ERROR: dist/ is out of date relative to src/." >&2
                echo "To regenerate, run from the repo root:" >&2
                echo "  chmod -R u+w dist 2>/dev/null; rm -rf dist && cp -r $expected/. dist/" >&2
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
