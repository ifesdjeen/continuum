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
scan_entire_keyspace(leveldb_t*             db,
                     leveldb_readoptions_t* roptions,
                     void (*append)(const char* key, size_t key_len,
                                    const char* val, size_t val_len)) {
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek_to_first(iter);

  while(leveldb_iter_valid(iter)) {
    size_t current_key_len;
    const char* current_key = leveldb_iter_key(iter, &current_key_len);

    size_t current_val_len;
    const char* current_val = leveldb_iter_value(iter, &current_val_len);

    append(current_key, current_key_len, current_val, current_val_len);
    leveldb_iter_next(iter);
  }

  leveldb_iter_destroy(iter);
  fflush(stdout);
}

void
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           int (*compare)(void*,
                          const char* a, size_t alen),
           void (*append)(const char* key, size_t key_len,
                          const char* val, size_t val_len)) {
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek(iter, start_at, start_at_len);

  while(leveldb_iter_valid(iter)) {
    size_t current_key_len;
    const char* current_key = leveldb_iter_key(iter, &current_key_len);

    if(compare(NULL, current_key, current_key_len) > 0) {
      break;
    }

    size_t current_val_len;
    const char* current_val = leveldb_iter_value(iter, &current_val_len);

    append(current_key, current_key_len, current_val, current_val_len);

    leveldb_iter_next(iter);
  }

  leveldb_iter_destroy(iter);
}

void
scan_range_butfirst(leveldb_t*             db,
                    leveldb_readoptions_t* roptions,
                    const char*            start_at,
                    size_t                 start_at_len,
                    int (*compare)(void*,
                                   const char* a, size_t alen),
                    void (*append)(const char* key, size_t key_len,
                                   const char* val, size_t val_len)) {
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek(iter, start_at, start_at_len);

  leveldb_iter_next(iter);
  if(leveldb_iter_valid(iter)) {
    while(leveldb_iter_valid(iter)) {
      size_t current_key_len;
      const char* current_key = leveldb_iter_key(iter, &current_key_len);

      if(compare(NULL, current_key, current_key_len) > 0) {
        break;
      }

      size_t current_val_len;
      const char* current_val = leveldb_iter_value(iter, &current_val_len);

      append(current_key, current_key_len, current_val, current_val_len);

      leveldb_iter_next(iter);
    }
  }

  leveldb_iter_destroy(iter);
}

void
scan_range_butlast(leveldb_t*             db,
                   leveldb_readoptions_t* roptions,
                   const char*            start_at,
                   size_t                 start_at_len,
                   int (*compare)(void*,
                                  const char* a, size_t alen),
                   void (*append)(const char* key, size_t key_len,
                                  const char* val, size_t val_len)) {
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek(iter, start_at, start_at_len);

  while(leveldb_iter_valid(iter)) {
    size_t current_key_len;
    const char* current_key = leveldb_iter_key(iter, &current_key_len);

    if(compare(NULL, current_key, current_key_len) == 0) {
      break;
    }

    size_t current_val_len;
    const char* current_val = leveldb_iter_value(iter, &current_val_len);

    append(current_key, current_key_len, current_val, current_val_len);

    leveldb_iter_next(iter);
  }

  leveldb_iter_destroy(iter);
}

void
scan_open_end(leveldb_t*             db,
              leveldb_readoptions_t* roptions,
              const char*            start_at,
              size_t                 start_at_len,
              void (*append)(const char* key, size_t key_len,
                             const char* val, size_t val_len)) {
  {
    leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
    leveldb_iter_seek(iter, start_at, start_at_len);

    while(leveldb_iter_valid(iter)) {
      size_t current_key_len;
      const char* current_key = leveldb_iter_key(iter, &current_key_len);

      size_t current_val_len;
      const char* current_val = leveldb_iter_value(iter, &current_val_len);

      append(current_key, current_key_len, current_val, current_val_len);
      leveldb_iter_next(iter);
    }

    leveldb_iter_destroy(iter);
  }
}


void
scan_open_end_butfirst(leveldb_t*             db,
                       leveldb_readoptions_t* roptions,
                       const char*            start_at,
                       size_t                 start_at_len,
                       void (*append)(const char* key, size_t key_len,
                                      const char* val, size_t val_len)) {

  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek(iter, start_at, start_at_len);

  leveldb_iter_next(iter);
  if(leveldb_iter_valid(iter)) {
    while(leveldb_iter_valid(iter)) {
      size_t current_key_len;
      const char* current_key = leveldb_iter_key(iter, &current_key_len);

      size_t current_val_len;
      const char* current_val = leveldb_iter_value(iter, &current_val_len);

      append(current_key, current_key_len, current_val, current_val_len);
      leveldb_iter_next(iter);
    }
  }

  leveldb_iter_destroy(iter);
}
