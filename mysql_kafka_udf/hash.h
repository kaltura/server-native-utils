#ifndef __HASH_H__
#define __HASH_H__

#include <stddef.h>
#include "list_entry.h"

// constants
#define HASH_SIZE (101)
#define container_of(ptr, type, member) (type *)((char *)(ptr) - offsetof(type, member))

// typedefs
typedef struct
{
	list_entry_t link;
	char* key;
	size_t key_length;
} hash_entry_t;

typedef struct
{
	list_entry_t heads[HASH_SIZE];
} hash_table_t;

// functions
void hash_init(hash_table_t* hash);

hash_entry_t* hash_lookup(hash_table_t* hash, const char* key, size_t key_length);

void hash_add(hash_table_t* hash, hash_entry_t* node);

#endif // __HASH_H__
