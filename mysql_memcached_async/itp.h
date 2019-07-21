#ifndef __ITP_H__
#define __ITP_H__

// includes
#include <semaphore.h>
#include "common.h"

/*
    ITP = Inter Thread Pipe
        Fast way to transfer buffers between threads with zero copy
        Assumes a single reader thread and a single writer thread
*/

// typedefs
typedef struct {
    u_char* ptr;
    size_t size;
    uint32_t flags;
} itp_buffer_t;

typedef struct {
    itp_buffer_t* start;        // fixed
    itp_buffer_t* end;          // fixed
    itp_buffer_t* write;        // used only by writer thread
    itp_buffer_t* read;         // used only by reader thread
    sem_t free_slot_sem;        // signalled by reader, waited by writer
    sem_t data_avail_sem;       // signalled by writer, waited by reader
} itp_t;

// functions
bool_t itp_init(itp_t* state, size_t size);

bool_t itp_write(itp_t* state, itp_buffer_t* buffer, bool_t wait);

bool_t itp_read(itp_t* state, itp_buffer_t* buffer, bool_t wait);

#endif // __ITP_H__
