#ifndef __BUFFER_POOL_H__
#define __BUFFER_POOL_H__

// includes
#include "common.h"

// typedefs
typedef struct list_node_s {
	struct list_node_s* next;
} list_node_t;

typedef struct {
	size_t size;
	list_node_t* free_head;
	pthread_mutex_t lock;
} buffer_pool_t;

// functions
bool_t buffer_pool_init(buffer_pool_t* pool, size_t size);

u_char* buffer_pool_alloc(buffer_pool_t* pool);

void buffer_pool_free(buffer_pool_t* pool, u_char* buffer);

#endif // __BUFFER_POOL_H__
