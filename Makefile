SHELL         := /usr/bin/env bash
NAME          := continuum
CABAL_SANDBOX ?= $(CURDIR)/.cabal-sandbox

CONFIGURED    := dist/setup-config

.PHONY: build
build: $(CONFIGURED)
	cabal build -j

cabal.sandbox.config:
	cabal sandbox init --sandbox=$(CABAL_SANDBOX)

.PHONY: deps
deps: cabal.sandbox.config
	cabal install -j --only-dep --enable-documentation --enable-test

.PHONY: clean
clean:
	cabal clean

.PHONY: prune
prune: clean
	cabal sandbox delete

dash:
	dash-haskell -c continuum.cabal -o docsets

$(CONFIGURED): cabal.sandbox.config deps $(NAME).cabal
	cabal configure --enable-test

