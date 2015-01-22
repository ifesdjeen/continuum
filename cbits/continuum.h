#ifndef CONTINUUM_H
#define CONTINUUM_H

#include <stdint.h>
#include <stddef.h>

#include "leveldb/c.h"

void
scan_entire_keyspace(leveldb_t*             db,
                     leveldb_readoptions_t* roptions,
                     void (*append)(const char* key, size_t key_len,
                                    const char* val, size_t val_len));

void
scan_range(leveldb_t*             db,
           leveldb_readoptions_t* roptions,
           const char*            start_at,
           size_t                 start_at_len,
           int (*compare)(void*,
                          const char* a, size_t alen),
           void (*append)(const char* key, size_t key_len,
                          const char* val, size_t val_len));

void
scan_range_butfirst(leveldb_t*             db,
                    leveldb_readoptions_t* roptions,
                    const char*            start_at,
                    size_t                 start_at_len,
                    int (*compare)(void*,
                                   const char* a, size_t alen),
                    void (*append)(const char* key, size_t key_len,
                                   const char* val, size_t val_len));
void
scan_open_end(leveldb_t*             db,
              leveldb_readoptions_t* roptions,
              const char*            start_at,
              size_t                 start_at_len,
              void (*append)(const char* key, size_t key_len,
                            const char* val, size_t val_len));

void
scan_open_end_butfirst(leveldb_t*             db,
                       leveldb_readoptions_t* roptions,
                       const char*            start_at,
                       size_t                 start_at_len,
                       void (*append)(const char* key, size_t key_len,
                                      const char* val, size_t val_len));

int
bitwise_compare(void* shared,
                const char* a, size_t alen,
                const char* b, size_t blen);

#endif /* CONTINUUM_H */
