{
  # The tools outside Python. Today that is tokei, which language_filter.py runs.
  # Python and its packages come from pyproject.toml, not from here, and CI does
  # not use this file (see .github/workflows/ci.yml).
  #
  #   nix develop               # a shell with tokei on PATH
  #   nix build .#tokei         # check that tokei resolves for this system
  description = "cregit-token-pipeline: tools outside Python";

  # nixos-26.05, which has tokei 14.0.0. nixos-unstable already has 15.
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/c56a87d8e41ce69bd2af57eb35dcc117a9dfca33";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSystem = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forEachSystem (pkgs: {
        tokei = pkgs.tokei;
        default = pkgs.tokei;
      });

      devShells = forEachSystem (pkgs: {
        default = pkgs.mkShell { packages = [ pkgs.tokei ]; };
      });
    };
}
