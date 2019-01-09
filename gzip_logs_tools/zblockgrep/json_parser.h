#ifndef __JSON_PARSER_H__
#define __JSON_PARSER_H__

// includes
#include <sys/types.h>
#include <stdint.h>

// enums
enum {
	JSON_NULL,
	JSON_BOOL,
	JSON_INT,
	JSON_FRAC,
	JSON_STRING,
	JSON_ARRAY,
	JSON_OBJECT,
};

enum {
	JSON_OK = 0,
	JSON_BAD_DATA = -1,
	JSON_ALLOC_FAILED = -2,
	JSON_BAD_LENGTH = -3,
	JSON_BAD_TYPE = -4,
};

// typedefs
typedef intptr_t bool_t;
typedef intptr_t json_status_t;
typedef void pool_t;

typedef struct {
	size_t len;
	char *data;
} str_t;

typedef struct {
	int64_t num;
	uint64_t denom;
} json_fraction_t;

typedef struct {
    void *elts;
    uintptr_t nelts;
    size_t size;
    uintptr_t nalloc;
	pool_t* pool;
} json_array_t;

typedef struct {
	int type;
	json_array_t items;
} json_array_value_t;

typedef json_array_t json_object_t;

typedef struct {
	int type;
	union {
		bool_t boolean;
		json_fraction_t num;
		str_t str;			// Note: the string is escaped (e.g. may contain \n, \t etc.)
		json_array_value_t arr;
		json_object_t obj;	// of json_key_value_t
	} v;
} json_value_t;

typedef struct {
	uintptr_t key_hash;
	str_t key;
	json_value_t value;
} json_key_value_t;

// functions
json_status_t json_parse(
	pool_t* pool, 
	char* string, 
	json_value_t* result, 
	char* error, 
	size_t error_size);

json_status_t json_decode_string(str_t* dest, str_t* src);

#endif // __JSON_PARSER_H__
