#include <stdlib.h>
#include <string.h>
#include <ini.h>
#include "curl_ext_module.h"
#include "curl_ext_s3.h"
#include "curl_ext.h"


static curl_ext_module_t* modules[] = {
	&curl_ext_s3,
};


struct curl_ext_conf_s {
	void** ctx;
};


static int
curl_ext_conf_handler(void* data, const char* section, const char* name, const char* value)
{
	curl_ext_module_t* cur;
	curl_ext_conf_t* conf = data;
	int i;

	for (i = 0; i < array_entries(modules); i++)
	{
		cur = modules[i];

		if (strcasecmp(section, cur->section) != 0)
		{
			continue;
		}

		return cur->conf_handler(conf->ctx[i], name, value);
	}

	return 1;
}

curl_ext_conf_t*
curl_ext_conf_init(const char* file_name)
{
	curl_ext_conf_t* conf;
	int i;

	conf = calloc(sizeof(*conf) + array_entries(modules) * sizeof(void*), 1);
	if (conf == NULL)
	{
		error(0, "failed to alloc conf");
		goto failed;
	}

	conf->ctx = (void*)(conf + 1);

	for (i = 0; i < array_entries(modules); i++)
	{
		conf->ctx[i] = modules[i]->conf_create();
		if (conf->ctx[i] == NULL)
		{
			goto failed;
		}
	}

	if (file_name == NULL)
	{
		return conf;
	}

	if (ini_parse(file_name, curl_ext_conf_handler, conf))
	{
		error(0, "failed to parse ini %s", file_name);
		goto failed;
	}

	for (i = 0; i < array_entries(modules); i++)
	{
		if (!modules[i]->conf_init(conf->ctx[i]))
		{
			goto failed;
		}
	}

	return conf;

failed:

	curl_ext_conf_free(conf);
	return NULL;
}

void
curl_ext_conf_free(curl_ext_conf_t* conf)
{
	int i;

	if (conf == NULL)
	{
		return;
	}

	for (i = 0; i < array_entries(modules); i++)
	{
		if (conf->ctx[i] == NULL)
		{
			continue;
		}

		modules[i]->conf_free(conf->ctx[i]);
	}

	free(conf);
}

bool_t
curl_ext_ctx_init(curl_ext_ctx_t* ctx, curl_ext_conf_t* conf, str_t* url, CURL* curl)
{
	curl_ext_module_t* cur;
	int i;

	for (i = 0; i < array_entries(modules); i++)
	{
		cur = modules[i];

		if (strncmp(url->data, cur->url_prefix.data, cur->url_prefix.len) != 0)
		{
			continue;
		}

		url->data += cur->url_prefix.len;
		url->len -= cur->url_prefix.len;

		ctx->ctx = cur->init(conf->ctx[i], url, curl);
		if (ctx->ctx == NULL)
		{
			return FALSE;
		}

		ctx->module = i;
		break;
	}

	return TRUE;
}

void
curl_ext_ctx_free(curl_ext_ctx_t* ctx)
{
	if (ctx->ctx == NULL)
	{
		return;
	}

	modules[ctx->module]->free(ctx->ctx);
	ctx->ctx = NULL;
}
