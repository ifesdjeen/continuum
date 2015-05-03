#include "continuum.h"
#include "leveldb/c.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

int
bitwise_compare(void* shared,
                const char* a, size_t alen,
                const char* b, size_t blen) {
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  return r;
}

void
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           const char*            end_at,
           size_t                 end_at_len,
           void (*append)(const char* key, size_t key_len,
                          const char* val, size_t val_len)) {
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);

  if (start_at_len == 0) {
    leveldb_iter_seek_to_first(iter);
  } else {
    leveldb_iter_seek(iter, start_at, start_at_len);
  }

  while(leveldb_iter_valid(iter)) {
    size_t current_key_len;
    const char* current_key = leveldb_iter_key(iter, &current_key_len);

    if(bitwise_compare(current_key, current_key_len,
                       end_at, end_at_len) > 0) {
      break;
    }

    size_t current_val_len;
    const char* current_val = leveldb_iter_value(iter, &current_val_len);

    append(current_key, current_key_len, current_val, current_val_len);
    leveldb_iter_next(iter);
  }

  leveldb_iter_destroy(iter);
}
