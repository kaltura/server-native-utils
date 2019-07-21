#include <pthread.h>
#include <stdlib.h>
#include "buffer_pool.h"

bool_t
buffer_pool_init(buffer_pool_t* pool, size_t size)
{
    if (pthread_mutex_init(&pool->lock, NULL) != 0)
    {
        return FALSE;
    }

    pool->size = size;
    pool->free_head = NULL;

    return TRUE;
}

u_char*
buffer_pool_alloc(buffer_pool_t* pool)
{
    u_char* result;

    pthread_mutex_lock(&pool->lock);
    if (pool->free_head != NULL)
    {
        result = (u_char*)pool->free_head;
        pool->free_head = pool->free_head->next;
        pthread_mutex_unlock(&pool->lock);
        return result;
    }
    
    pthread_mutex_unlock(&pool->lock);
    return malloc(pool->size);
}

void
buffer_pool_free(buffer_pool_t* pool, u_char* buffer)
{
    pthread_mutex_lock(&pool->lock);
    ((list_node_t*)buffer)->next = pool->free_head;
    pool->free_head = (list_node_t*)buffer;
    pthread_mutex_unlock(&pool->lock);
}
