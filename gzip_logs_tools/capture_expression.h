#ifndef __CAPTURE_EXPRESSION_H__
#define __CAPTURE_EXPRESSION_H__

// includes
#include <stdlib.h>

// constants
#define MAX_CAPTURES (9)	// $1..$9

// typedefs
typedef struct {
	int capture_index;
	const char* data;
	size_t len;
} capture_expression_t;

// functions
capture_expression_t* parse_capture_expression(
	const char* str,
	size_t len,
	int* max_capture_index);

size_t eval_capture_expression(
	capture_expression_t* capture_expression,
	char* dest,
	size_t dest_size,
	const char* buffer,
	int* captures);

#endif // __CAPTURE_EXPRESSION_H__
