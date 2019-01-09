#ifndef __COMPRESSED_FILE_H__
#define __COMPRESSED_FILE_H__

// includes
#include <stdio.h>
#include <zlib.h>
#include "common.h"

// constants
#define INPUT_CHUNK_SIZE (1048576)
#define OUTPUT_CHUNK_SIZE (1048576)

// enums
enum {
	PROCESS_SUCCESS,
	PROCESS_DONE,
	PROCESS_ERROR,
	PROCESS_RESYNC,
};

// typedefs
typedef void (*process_chunk_callback_t)(
	void* context, 
	u_char* pos, 
	size_t size);

typedef struct {
	char* file_name;
	FILE* source;
	long limit;
	z_stream strm;
	u_char in[INPUT_CHUNK_SIZE];
	u_char out[OUTPUT_CHUNK_SIZE];
} compressed_file_state_t;

// functions
bool_t compressed_file_init(compressed_file_state_t* state, const char* file_name);

void compressed_file_free(compressed_file_state_t* state);

long compressed_file_get_pos(compressed_file_state_t* state);

int compressed_file_process_segment(compressed_file_state_t* state, process_chunk_callback_t callback, void* context);

int compressed_file_resync(compressed_file_state_t* state);

#endif // __COMPRESSED_FILE_H__
