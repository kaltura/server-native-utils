#include <stdlib.h>
#include "itp.h"

bool_t
itp_init(itp_t* state, size_t size)
{
    int rc;

    state->start = malloc(sizeof(itp_buffer_t) * size);
    if (state->start == NULL)
    {
        return FALSE;
    }
    
    state->end = state->start + size;
    state->write = state->start;
    state->read = state->start;
    
    rc = sem_init(&state->free_slot_sem, 0, size);
    if (rc != 0)
    {
        return FALSE;
    }
    
    rc = sem_init(&state->data_avail_sem, 0, 0);
    if (rc != 0)
    {
        return FALSE;
    }

    return TRUE;
}

bool_t
itp_write(itp_t* state, itp_buffer_t* buffer, bool_t wait)
{
    itp_buffer_t* next_write_pos;
    int rc;

    if (wait)
    {
        rc = sem_wait(&state->free_slot_sem);
    }
    else
    {
        rc = sem_trywait(&state->free_slot_sem);
    }
    
    if (rc != 0)
    {
        return FALSE;
    }
    
    next_write_pos = state->write + 1;
    if (next_write_pos >= state->end)
    {
        next_write_pos = state->start;
    }

    *state->write = *buffer;
    state->write = next_write_pos;
    
    rc = sem_post(&state->data_avail_sem);
    if (rc != 0)
    {
        return FALSE;
    }
    
    return TRUE;
}

bool_t
itp_read(itp_t* state, itp_buffer_t* buffer, bool_t wait)
{
    itp_buffer_t* next_read_pos;
    int rc;

    if (wait)
    {
        rc = sem_wait(&state->data_avail_sem);
    }
    else
    {
        rc = sem_trywait(&state->data_avail_sem);
    }
    
    if (rc != 0)
    {
        return FALSE;
    }
    
    next_read_pos = state->read + 1;
    if (next_read_pos >= state->end)
    {
        next_read_pos = state->start;
    }

    *buffer = *state->read;
    state->read = next_read_pos;

    rc = sem_post(&state->free_slot_sem);
    if (rc != 0)
    {
        return FALSE;
    }
    
    return TRUE;
}
