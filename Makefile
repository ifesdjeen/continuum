LIBHSLEVELDB = dist/build/*.a
LIBHYPERLEVELDB = /usr/local/lib/libhyperleveldb*

export LD_LIBRARY_PATH="/usr/local/lib"

travis : $(LIBHYPERLEVELDB)

$(LIBHYPERLEVELDB) :
		(cd /tmp;                                               \
     git clone --depth=50 --branch=master git://github.com/rescrv/HyperLevelDB.git hyperleveldb; \
     cd hyperleveldb;                                       \
		 autoreconf -i;                                         \
		 ./configure;                                           \
		 make;                                                  \
		 make install;                                          \
		 sudo cp -P ./libhyperleveldb.* /usr/local/lib;         \
		 sudo cp -r include/hyperleveldb /usr/local/include/;   \
		 ls -lah /usr/local/lib/ )
