# -*- compile-command: "nix-build"; -*-
{ pkgs ? import <nixpkgs> {} }:
pkgs.callPackage ./derivation.nix {}
