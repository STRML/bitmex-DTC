# DTC to BitMEX API adapter

## Installation

From scratch from a debian install:

* Install OCaml: `apt-get install ocaml-nox`
* Install OPAM from source: `git://github.com/ocaml/opam`, then follow installation instructions
* Use OCaml 4.03+flambda: `opam switch 4.03+flambda`
* Add Janestreet opam repository: `opam repo add janestreet git://github.com/janestreet/opam-repository`
* Add Bitsouk opam repository: `opam repo add bitsouk git://github.com/vbmithr/bs-opam-repository`
* Install depext: `opam install depext`, and use it to install system deps to some OCaml packages (LevelDB, SSL libraries, etc.)
* Install bs_api: `opam install bs_api`

The code should then compile, just do a `make` in this repo.