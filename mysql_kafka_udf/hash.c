#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include "hash.h"

// copied from nginx
static uint32_t
murmur_hash(const u_char *data, size_t len)
{
    uint32_t  h, k;

    h = 0 ^ len;

    while (len >= 4) {
        k  = data[0];
        k |= data[1] << 8;
        k |= data[2] << 16;
        k |= data[3] << 24;

        k *= 0x5bd1e995;
        k ^= k >> 24;
        k *= 0x5bd1e995;

        h *= 0x5bd1e995;
        h ^= k;

        data += 4;
        len -= 4;
    }

    switch (len) {
    case 3:
        h ^= data[2] << 16;
    case 2:
        h ^= data[1] << 8;
    case 1:
        h ^= data[0];
        h *= 0x5bd1e995;
    }

    h ^= h >> 13;
    h *= 0x5bd1e995;
    h ^= h >> 15;

    return h;
}

void
hash_init(hash_table_t* hash)
{
	size_t i;
	
	for (i = 0; i < HASH_SIZE; i++)
	{
		initialize_list_head(&hash->heads[i]);
	}
}

hash_entry_t*
hash_lookup(hash_table_t* hash, const char* key, size_t key_length)
{
	list_entry_t* list;
	list_entry_t* cur;
	hash_entry_t* hash_entry;
	size_t hash_value;
	
	hash_value = murmur_hash((const u_char*)key, key_length) % HASH_SIZE;
	list = &hash->heads[hash_value];
	
	for (cur = list->next; cur != list; cur = cur->next)
	{
		hash_entry = container_of(cur, hash_entry_t, link);
		if (hash_entry->key_length == key_length && 
			memcmp(hash_entry->key, key, key_length) == 0)
		{
			return hash_entry;
		}
	}
	
	return NULL;
}

void
hash_add(hash_table_t* hash, hash_entry_t* node)
{
	list_entry_t* list;
	size_t hash_value;
	
	hash_value = murmur_hash((const u_char*)node->key, node->key_length) % HASH_SIZE;
	list = &hash->heads[hash_value];

	insert_tail_list(list, &node->link);
}
