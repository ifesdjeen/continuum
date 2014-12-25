#include "continuum.h"
#include "leveldb/c.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define RESULTSET_BASE 100

static int compare_fn(const char* a, size_t alen,
                   const char* b, size_t blen) {
  int n = (alen < blen) ? alen : blen;
  int r = memcmp(a, b, n);
  if (r == 0) {
    if (alen < blen) r = -1;
    else if (alen > blen) r = +1;
  }
  return r;
}

db_results_t*
scan_entire_keyspace(leveldb_t* db,
                     leveldb_readoptions_t* roptions) {
  key_value_pair_t *kvps = (key_value_pair_t *)malloc(RESULTSET_BASE * sizeof(key_value_pair_t));

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

  db_results_t* db_result = calloc(1, sizeof(db_results_t));
  db_result->results = kvps;
  db_result->count = count;

  return db_result;
}

db_results_t*
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           const char*            end_at,
           size_t                 end_at_len,
           int (*compare)(void*,
                          const char* a, size_t alen,
                          const char* b, size_t blen)) {
  key_value_pair_t *kvps = (key_value_pair_t *)malloc(RESULTSET_BASE * sizeof(key_value_pair_t));

  int count = 0;
  {
    leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
    leveldb_iter_seek(iter, start_at, start_at_len);

    while(leveldb_iter_valid(iter)) {
      size_t current_key_len;
      const char* current_key = leveldb_iter_key(iter, &current_key_len);

      /* printf("%i", compare(NULL, current_key, current_key_len, end_at, end_at_len)); */
      /* printf("%i\n", compare_fn(current_key, current_key_len, end_at, end_at_len)); */
      if(compare(NULL, current_key, current_key_len, end_at, end_at_len) > 0) {
        break;
      }

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

  db_results_t* db_result = calloc(1, sizeof(db_results_t));
  db_result->results = kvps;
  db_result->count = count;

  return db_result;
}

void
free_db_results(db_results_t* dbr) {
  if (dbr == NULL)
    return;

  free(dbr->results);
  free(dbr);
}
