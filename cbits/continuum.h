#ifndef CONTINUUM_H
#define CONTINUUM_H

#include <stdint.h>
#include <stddef.h>

#include "leveldb/c.h"

int
bitwise_compare(void* shared,
                const char* a, size_t alen,
                const char* b, size_t blen);

int
constantly_true(const char* b, size_t blen);

void
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           void (*append)(const char* key, size_t key_len,
                          const char* val, size_t val_len));



#endif /* CONTINUUM_H */
