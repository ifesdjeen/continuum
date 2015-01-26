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
scan(leveldb_t*             db,
     leveldb_readoptions_t* roptions,
     const char*            start_at,
     size_t                 start_at_len,
     int (*compare)(const char* a, size_t alen),
     void (*append)(const char* key, size_t key_len,
                    const char* val, size_t val_len),
     int                    skip_first);



#endif /* CONTINUUM_H */
