# -*- compile-command: "nix-build"; -*-
{ lib, python3Packages }:
with python3Packages;
buildPythonPackage {
  pname = "python-snippets";
  version = "0.0.1";

  propagatedBuildInputs = [ pandas XlsxWriter ];

  nativeBuildInputs = [ipython];
  
  src = ./.;
}
