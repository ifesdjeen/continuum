LIBHSLEVELDB = dist/build/*.a

travis :
		(cd /tmp; \
			git clone https://code.google.com/p/leveldb/; \
			cd leveldb; \
			make; \
			sudo mv ./libleveldb* /usr/local/lib; \
			sudo cp -a ./include/leveldb /usr/local/include)
