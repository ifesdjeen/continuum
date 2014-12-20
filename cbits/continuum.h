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

#endif /* CONTINUUM_H */
