{
  description = "bnuystore";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (sys:
      let pkgs = import nixpkgs {
            system = sys;
            overlays = [ (import rust-overlay) ];
          };
          rust = pkgs.rust-bin.stable.latest.default.override {
            extensions = [ "rust-src" "rust-analyzer" ];
          };
          platform = pkgs.makeRustPlatform {
            rustc = rust;
            cargo = rust;
          };
      in rec {
        # includes bin/bnuystore-diagnose,front-node,storage-node
        packages.bnuystore = platform.buildRustPackage {
          name = "bnuystore";
          src = ./.;
          cargoLock = { lockFile = ./Cargo.lock; };
          buildFeatures = [ "front-node" ];

          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.openssl ];
        };
        devShells.default = pkgs.mkShell {
          packages = [ rust ];
          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [ pkgs.openssl ];
        };
      }
    );
}
