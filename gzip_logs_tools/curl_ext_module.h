#ifndef __CURL_EXT_MODULE_H__
#define __CURL_EXT_MODULE_H__

// includes
#include <curl/curl.h>
#include "common.h"


typedef struct {
	const char* section;
	void* (*conf_create)();
	bool_t (*conf_init)(void* conf);
	void (*conf_free)(void* conf);
	int (*conf_handler)(void* data, const char* name, const char* value);

	str_t url_prefix;
	void* (*init)(void* conf, str_t* url, CURL* curl);
	void (*free)(void* ctx);
} curl_ext_module_t;

#endif // __CURL_EXT_MODULE_H__
