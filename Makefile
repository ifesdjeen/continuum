NAME            := leveldb-haskell
CABAL_SANDBOX   ?= $(CURDIR)/.cabal-sandbox
LIBHSLEVELDB    := dist/build/*.a
LIBHYPERLEVELDB := /usr/local/lib/libhyperleveldb*
CONFIGURED      := dist/setup-config
export LD_LIBRARY_PATH="/usr/local/lib"

cabal.sandbox.config:
	cabal sandbox init --sandbox=$(CABAL_SANDBOX)

.PHONY: deps
deps: cabal.sandbox.config
	cabal install -j --dependencies-only --enable-documentation --enable-tests

$(CONFIGURED): cabal.sandbox.config deps $(NAME).cabal
	cabal configure --enable-test --enable-bench

.PHONY: clean
clean:
	cabal clean

.PHONY: prune
prune: clean
	cabal sandbox delete

travis : $(LIBHYPERLEVELDB)

$(LIBHYPERLEVELDB) :
		(cd /tmp;                                                                                    \
     git clone --depth=50 --branch=master git://github.com/rescrv/HyperLevelDB.git hyperleveldb; \
     cd hyperleveldb;                                                                            \
		 autoreconf -i;                                                                              \
		 ./configure;                                                                                \
		 make;                                                                                       \
		 sudo make install;                                                                          \
		 sudo ldconfig;                                                                              \
		 sudo cp -P ./libhyperleveldb.* /usr/local/lib;                                              \
		 sudo cp -r include/hyperleveldb /usr/local/include/;                                        \
		 ls -lah /usr/local/lib/)

$(CONFIGURED): cabal.sandbox.config deps $(NAME).cabal
	cabal configure --enable-test --enable-bench

reconf:
	cabal install -fBinaries --enable-tests --only-dependencies && \
	cabal configure -fBinaries --enable-tests
