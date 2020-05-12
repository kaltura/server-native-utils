#ifndef __COMPRESSED_FILE_H__
#define __COMPRESSED_FILE_H__

// includes
#include <curl/curl.h>
#include <zlib.h>
#include "curl_ext.h"

// constants
#define OUTPUT_CHUNK_SIZE (1048576)

// typedefs
typedef struct {

	void (*process_chunk)(void* context, u_char* pos, size_t size);

	void (*resync)(void* context, long pos);

	void (*segment_end)(void* context, long pos, bool_t error);
} compressed_file_observer_t;

typedef struct {
	char* url;
	char* input_url;
	compressed_file_observer_t observer;
	void* context;

	int state;
	long cur_pos;
	unsigned short last_word;

	CURL* curl;
	z_stream strm;

	curl_ext_ctx_t curl_ext;

	u_char out[OUTPUT_CHUNK_SIZE];
} compressed_file_state_t;

// functions
long compressed_file_init(compressed_file_state_t* state, curl_ext_conf_t* conf, const char* url, compressed_file_observer_t* observer, void* context);

void compressed_file_free(compressed_file_state_t* state);

bool_t compressed_file_process(compressed_file_state_t* state);

#endif // __COMPRESSED_FILE_H__
