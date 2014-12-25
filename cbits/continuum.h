#ifndef CONTINUUM_H
#define CONTINUUM_H

#include <stdint.h>
#include <stddef.h>

#include "leveldb/c.h"

struct key_value_pair_s
{
  const char* key;
  size_t key_len;
  const char* val;
  size_t val_len;
};

typedef struct key_value_pair_s key_value_pair_t;


struct db_results_s
{
  key_value_pair_t* results;
  // TODO: Maybe unsigned?
  int count;
};

typedef struct db_results_s db_results_t;

db_results_t*
scan_entire_keyspace(leveldb_t* db,
                     leveldb_readoptions_t* roptions);

db_results_t*
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           const char*            end_at,
           size_t                 end_at_len,
           int (*compare)(void*,
                          const char* a, size_t alen,
                          const char* b, size_t blen));

void
free_db_results(db_results_t* ptr);
#endif /* CONTINUUM_H */
