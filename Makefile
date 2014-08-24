LIBHSLEVELDB = dist/build/*.a
LIBLEVELDB   = /usr/local/lib/libleveldb*

LD_LIBRARY_PATH="/usr/local/lib"

travis : $(LIBLEVELDB)

$(LIBLEVELDB) :
		(cd /tmp;                                               \
			git clone https://code.google.com/p/leveldb/;         \
			cd leveldb;                                           \
			make;                                                 \
			sudo cp --preserve=links libleveldb.* /usr/local/lib; \
			ls -lah /usr/local/lib/ \
			sudo cp -r include/leveldb /usr/local/include/)
