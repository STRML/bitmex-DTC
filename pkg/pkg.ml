#!/usr/bin/env ocaml
#use "topfind"
#require "topkg"
open Topkg

let () =
  Pkg.describe "bitmex" @@ fun c ->
  Ok [
    Pkg.bin "src/bitmex";
    Pkg.bin "src/bitmex_data";
  ]
