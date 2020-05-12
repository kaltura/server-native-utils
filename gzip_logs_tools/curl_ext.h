#ifndef __CURL_EXT_H__
#define __CURL_EXT_H__

#include "common.h"


typedef struct curl_ext_conf_s curl_ext_conf_t;

typedef struct {
	void* ctx;
	int module;
} curl_ext_ctx_t;


curl_ext_conf_t* curl_ext_conf_init(const char* file_name);

void curl_ext_conf_free(curl_ext_conf_t* conf);

bool_t curl_ext_ctx_init(curl_ext_ctx_t* ctx, curl_ext_conf_t* conf, str_t* url, CURL* curl);

void curl_ext_ctx_free(curl_ext_ctx_t* ctx);

#endif // __CURL_EXT_H__
