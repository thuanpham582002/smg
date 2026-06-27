{
  description = "Shepherd Model Gateway development flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            cargo
            clang
            cmake
            git
            openssl
            pkg-config
            protobuf
            rustc
          ];

          env = {
            LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
            OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";
            OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
            PROTOC = "${pkgs.protobuf}/bin/protoc";
            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
          };
        };

        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "smg";
          version = "1.6.0";
          src = self;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = with pkgs; [
            cmake
            pkg-config
            protobuf
          ];
          buildInputs = with pkgs; [
            openssl
          ];
          env = {
            OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";
            OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          cargoBuildFlags = [ "-p" "smg" ];
          cargoTestFlags = [ "-p" "smg" ];
        };
      });
}
