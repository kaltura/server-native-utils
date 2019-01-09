#ifndef __FILTER_H__
#define __FILTER_H__

// includes
#include "../common.h"

// typedefs
typedef struct filter_base_s filter_base_t;

// functions
filter_base_t* filter_parse(char* str, char* error, size_t error_size);
bool_t filter_eval(filter_base_t* filter, char* data, size_t len);

#endif // __FILTER_H__
