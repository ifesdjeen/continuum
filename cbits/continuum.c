#include "continuum.h"
#include "leveldb/c.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#define RESULTSET_BASE 100

db_results_t*
scan_entire_keyspace(leveldb_t* db,
                     leveldb_readoptions_t* roptions) {
  key_value_pair_t *kvps = (key_value_pair_t *)malloc(100 * sizeof(key_value_pair_t));

  int count = 0;
  {
    leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
    leveldb_iter_seek_to_first(iter);

    while(leveldb_iter_valid(iter)) {
      kvps[count].key = leveldb_iter_key(iter, &kvps[count].key_len);
      kvps[count].val = leveldb_iter_value(iter, &kvps[count].val_len);

      count += 1;
      if(count == RESULTSET_BASE) {
        kvps = (key_value_pair_t *)realloc(kvps, (count + RESULTSET_BASE) * sizeof(key_value_pair_t));
      }
      leveldb_iter_next(iter);
    }

    leveldb_iter_destroy(iter);
  }

  db_results_t *db_result = calloc(1, sizeof(db_results_t));
  db_result->results = kvps;
  db_result->count = count;
  return db_result;
}
