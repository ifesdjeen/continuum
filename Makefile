LIBHSLEVELDB = dist/build/*.a
LIBHYPERLEVELDB = /usr/local/lib/libhyperleveldb*

export LD_LIBRARY_PATH="/usr/local/lib"

travis : $(LIBHYPERLEVELDB)

$(LIBHYPERLEVELDB) :
		(cd /tmp;                                               \
			git@github.com:rescrv/HyperLevelDB.git;               \
			cd HyperLevelDB;                                      \
		  autoreconf -i;                                        \
		  ./configure;                                          \
		  make;                                                 \
		  make install;                                         \
		  ldconfig;                                             \
			sudo cp -P ./libhyperleveldb.* /usr/local/lib;        \
			sudo cp -r include/hyperleveldb /usr/local/include/;  \
			ls -lah /usr/local/lib/ )
