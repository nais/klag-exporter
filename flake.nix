{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane.url = "github:ipetkov/crane";

    flake-utils.url = "github:numtide/flake-utils";

    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

  };

  outputs =
    { self, ... }@inputs:
    inputs.flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import inputs.nixpkgs { localSystem = { inherit system; }; };
        inherit (pkgs) lib;

        craneLib = inputs.crane.mkLib pkgs;
        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;
          buildInputs = [
            pkgs.openssl
            pkgs.cyrus_sasl
            pkgs.curlFull
          ];
          nativeBuildInputs = [
            pkgs.cmake
            pkgs.pkg-config
          ]
          ++ lib.optionals pkgs.stdenv.isDarwin [ pkgs.libiconv ];
        };

        cargoDetails = pkgs.lib.importTOML ./Cargo.toml;
        inherit (cargoDetails.package) name version;
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        rust-prog = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            meta.mainProgram = name; # This is default in cargo
          }
        );
        dockerTag =
          if lib.hasAttr "rev" self then "${lib.toString self.revCount}-${self.shortRev}" else "gitDirty";
        tag = "${version}-${dockerTag}";
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rust-analyzer
            cargo-watch
            clippy
            rustfmt

            kubernetes-helm
          ];
          inputsFrom = [ rust-prog ];
        };
        formatter = inputs.treefmt-nix.lib.mkWrapper pkgs {
          programs.nixfmt.enable = true;
          programs.rustfmt.enable = true;
        };
        packages.default = rust-prog;
        packages.docker = pkgs.dockerTools.buildImage rec {
          inherit name tag;
          uid = 6969;
          gid = 6969;
          config = {
            Expose = "8000";
            Workdir = "/app";
            User = "${lib.toString uid}:${lib.toString gid}";

            Entrypoint = [ (lib.getExe rust-prog) ];
            Cmd = [
              "--config"
              "/app/config.toml"
            ];
          };
        };
      }
    );
}
