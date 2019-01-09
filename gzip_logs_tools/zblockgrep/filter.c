#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <pcre.h>
#include "json_parser.h"
#include "filter.h"

// utility functions
static int 
filter_memcasecmp(const char *s1, const char *s2, size_t len)
{
	unsigned char c1, c2;

	if (!len)
	{
		return 0;
	}

	do 
	{
		c1 = *s1++;
		c2 = *s2++;
		if (c1 == c2)
		{
			continue;
		}

		c1 = tolower(c1);
		c2 = tolower(c2);
		if (c1 != c2)
		{
			break;
		}
	} while (--len);
	return (int)c1 - (int)c2;
}

static char *
filter_strpos(str_t* haystack, str_t* needle)
{
	char  c1, c2;
	char* s1 = haystack->data;
	char* s1_end = haystack->data + haystack->len - needle->len;
	char* s2 = needle->data + 1;
	size_t s2_len = needle->len - 1;

	c2 = needle->data[0];

	do
	{
		do
		{
			if (s1 > s1_end)
			{
				return NULL;
			}

			c1 = *s1++;

		} while (c1 != c2);

	} while (memcmp(s1, s2, s2_len) != 0);

	return --s1;
}

static char *
filter_strcasepos(str_t* haystack, str_t* needle)
{
	char  c1, c2;
	char* s1 = haystack->data;
	char* s1_end = haystack->data + haystack->len - needle->len;
	char* s2 = needle->data + 1;
	size_t s2_len = needle->len - 1;

	c2 = tolower(needle->data[0]);

	do
	{
		do
		{
			if (s1 > s1_end)
			{
				return NULL;
			}

			c1 = tolower(*s1++);

		} while (c1 != c2);

	} while (filter_memcasecmp(s1, s2, s2_len) != 0);

	return --s1;
}

static json_value_t* 
filter_get_json_object_value(json_object_t* obj, const char* name, size_t name_len, int type)
{
	json_key_value_t* cur;
	json_key_value_t* end;
	
	for (cur = obj->elts, end = cur + obj->nelts; cur < end; cur++)
	{
		if (cur->key.len == name_len &&
			memcmp(cur->key.data, name, name_len) == 0 &&
			cur->value.type == type)
		{
			return &cur->value;
		}
	}
	
	return NULL;
}

static str_t* 
filter_get_json_object_string_value(json_object_t* obj, const char* name, size_t name_len)
{
	json_value_t* value;
	
	value = filter_get_json_object_value(obj, name, name_len, JSON_STRING);
	if (value == NULL)
	{
		return NULL;
	}
	return &value->v.str;
}

static bool_t 
filter_get_json_object_bool_value(json_object_t* obj, const char* name, size_t name_len, bool_t def)
{
	json_value_t* value;
	
	value = filter_get_json_object_value(obj, name, name_len, JSON_BOOL);
	if (value == NULL)
	{
		return def;
	}
	return value->v.boolean;
}

/// base filter
typedef struct filter_parse_ctx_s filter_parse_ctx_t;
typedef filter_base_t* (*filter_parse_func_t)(filter_parse_ctx_t* ctx, json_object_t* obj);
typedef bool_t (*filter_eval_func_t)(void* filter, str_t* block);

struct filter_base_s
{
	filter_eval_func_t eval;
};

struct filter_parse_ctx_s 
{
	char* error;
	size_t error_size;
};

static filter_base_t* filter_parse_object(filter_parse_ctx_t* ctx, json_object_t* obj);

static bool_t 
filter_eval_true(void* obj, str_t* block)
{
	return TRUE;
}

/// match filter
typedef struct
{
	filter_base_t base;
	str_t text;
} filter_match_t;

static bool_t 
filter_match_eval(void* obj, str_t* block)
{
	filter_match_t* filter = obj;
	
	return filter_strpos(block, &filter->text) != NULL;
}

static bool_t 
filter_match_eval_case(void* obj, str_t* block)
{
	filter_match_t* filter = obj;
	
	return filter_strcasepos(block, &filter->text) != NULL;
}

static filter_base_t*
filter_match_parse(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	filter_match_t* filter;
	str_t* text;
	
	text = filter_get_json_object_string_value(obj, "text", sizeof("text") - 1);
	if (text == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "match filter: missing text field");
		return NULL;
	}
	
	filter = malloc(sizeof(*filter) + text->len);
	if (filter == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "match filter: malloc failed");
		return NULL;
	}
	
	filter->text.data = (void*)(filter + 1);
	filter->text.len = 0;
	
	if (json_decode_string(&filter->text, text) != JSON_OK)
	{
		snprintf(ctx->error, ctx->error_size, "match filter: failed to decode string");
		return NULL;
	}
	
	if (text->len == 0)
	{
		filter->base.eval = &filter_eval_true;
	}
	else if (filter_get_json_object_bool_value(obj, "ignorecase", sizeof("ignorecase") - 1, TRUE))
	{
		filter->base.eval = &filter_match_eval_case;
	}
	else
	{
		filter->base.eval = &filter_match_eval;
	}

	return &filter->base;
}

/// regex filter
typedef struct
{
	filter_base_t base;
	pcre *code;
	pcre_extra *extra;
} filter_regex_t;

static bool_t 
filter_regex_eval(void* obj, str_t* block)
{
	filter_regex_t* filter = obj;
	
	return pcre_exec(
		filter->code, 
		filter->extra, 
		(const char *)block->data, 
		block->len, 
		0, 
		0, 
		NULL, 
		0) >= 0;
}

static filter_base_t*
filter_regex_parse(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	filter_regex_t* filter;
	str_t pattern_dec;
	str_t* pattern;
	const char *errstr;
	int erroff;
	int options;
	
	pattern = filter_get_json_object_string_value(obj, "pattern", sizeof("pattern") - 1);
	if (pattern == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "regex filter: missing pattern field");
		return NULL;
	}
	
	pattern_dec.data = malloc(pattern->len + 1);
	if (pattern_dec.data == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "regex filter: malloc failed");
		return NULL;
	}
	pattern_dec.len = 0;
	
	if (json_decode_string(&pattern_dec, pattern) != JSON_OK)
	{
		snprintf(ctx->error, ctx->error_size, "regex filter: failed to decode string");
		return NULL;
	}
	pattern_dec.data[pattern_dec.len] = '\0';
	
	filter = malloc(sizeof(*filter));
	if (filter == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "regex filter: malloc failed");
		return NULL;
	}
	
	options = 0;
	if (filter_get_json_object_bool_value(obj, "ignorecase", sizeof("ignorecase") - 1, TRUE))
	{
		options |= PCRE_CASELESS;
	}
	if (filter_get_json_object_bool_value(obj, "multiline", sizeof("multiline") - 1, FALSE))
	{
		options |= PCRE_MULTILINE;
	}
	if (filter_get_json_object_bool_value(obj, "dotall", sizeof("dotall") - 1, FALSE))
	{
		options |= PCRE_DOTALL;
	}
	if (filter_get_json_object_bool_value(obj, "ungreedy", sizeof("ungreedy") - 1, FALSE))
	{
		options |= PCRE_UNGREEDY;
	}
	
	filter->code = pcre_compile(pattern_dec.data, options, &errstr, &erroff, NULL);
	if (filter->code == NULL) 
	{
		if ((size_t) erroff == pattern_dec.len) 
		{
			snprintf(ctx->error, ctx->error_size, "regex filter: compile failed: %s in \"%s\"", errstr, pattern_dec.data);
		} 
		else 
		{
			snprintf(ctx->error, ctx->error_size, "regex filter: compile failed: %s in \"%s\" at \"%s\"", errstr, pattern_dec.data, pattern_dec.data + erroff);
		}

		return NULL;
	}

	filter->extra = pcre_study(filter->code, 0, &errstr);
	if (errstr != NULL) 
	{
		snprintf(ctx->error, ctx->error_size, "regex filter: study failed: %s", errstr);
	}
	
	filter->base.eval = &filter_regex_eval;
	return &filter->base;
}

/// not filter
typedef struct
{
	filter_base_t base;
	filter_base_t* filter;
} filter_not_t;

static bool_t 
filter_not_eval(void* obj, str_t* block)
{
	filter_base_t* filter = ((filter_not_t*)obj)->filter;
	
	return !filter->eval(filter, block);
}

static filter_base_t*
filter_not_parse(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	filter_not_t* filter;
	json_value_t* child;
	
	child = filter_get_json_object_value(obj, "filter", sizeof("filter") - 1, JSON_OBJECT);
	if (child == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "not filter: missing filter field");
		return NULL;
	}

	filter = malloc(sizeof(*filter));
	if (filter == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "not filter: malloc failed");
		return NULL;
	}

	filter->filter = filter_parse_object(ctx, &child->v.obj);
	if (filter->filter == NULL)
	{
		return NULL;
	}
	
	filter->base.eval = &filter_not_eval;
	return &filter->base;
}

/// and/or filters
typedef struct
{
	filter_base_t base;
	filter_base_t** filters;
} filter_and_or_t;

static filter_base_t*
filter_and_or_parse(filter_parse_ctx_t* ctx, json_object_t* obj, filter_eval_func_t eval)
{
	filter_and_or_t* filter;
	filter_base_t* cur_filter;
	filter_base_t** filters;
	json_value_t* arr;
	json_object_t* cur;
	json_object_t* end;
	json_array_t* items;
	
	arr = filter_get_json_object_value(obj, "filters", sizeof("filters") - 1, JSON_ARRAY);
	if (arr == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "and/or filter: missing filters field");
		return NULL;
	}
	
	if (arr->v.arr.type != JSON_OBJECT)
	{
		snprintf(ctx->error, ctx->error_size, "and/or filter: filters array must contain objects");
		return NULL;
	}
	
	items = &arr->v.arr.items;
	filter = malloc(sizeof(*filter) + sizeof(filter->filters[0]) * (items->nelts + 1));
	if (filter == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "and/or filter: malloc failed");
		return NULL;
	}
		
	filter->filters = filters = (void*)(filter + 1);
	
	cur = (json_object_t*)items->elts;
	end = cur + items->nelts;
	
	for (; cur < end; cur++)
	{
		cur_filter = filter_parse_object(ctx, cur);
		if (cur_filter == NULL)
		{
			return NULL;
		}
		
		*filters++ = cur_filter;
	}
	
	*filters = NULL;
	filter->base.eval = eval;	
	return &filter->base;
}

static bool_t 
filter_and_eval(void* obj, str_t* block)
{
	filter_base_t** filters = ((filter_and_or_t*)obj)->filters;
	filter_base_t* filter;
	
	for (;;)
	{
		filter = *filters;
		if (filter == NULL)
		{
			break;
		}
		
		if (!filter->eval(filter, block))
		{
			return FALSE;
		}
		
		filters++;
	}
	
	return TRUE;
}

static bool_t 
filter_or_eval(void* obj, str_t* block)
{
	filter_base_t** filters = ((filter_and_or_t*)obj)->filters;
	filter_base_t* filter;
	
	for (;;)
	{
		filter = *filters;
		if (filter == NULL)
		{
			break;
		}
		
		if (filter->eval(filter, block))
		{
			return TRUE;
		}
		
		filters++;
	}
	
	return FALSE;
}

static filter_base_t*
filter_and_parse(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	return filter_and_or_parse(ctx, obj, filter_and_eval);
}

static filter_base_t*
filter_or_parse(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	return filter_and_or_parse(ctx, obj, filter_or_eval);
}

/// base
typedef struct {
	str_t name;
	filter_parse_func_t parse;
} filter_def_t;

#define DEFINE_FILTER(name) { { sizeof(#name) - 1, #name }, &filter_ ## name ## _parse }

static filter_def_t filter_defs[] = {
	DEFINE_FILTER(match),
	DEFINE_FILTER(regex),
	DEFINE_FILTER(not),
	DEFINE_FILTER(and),
	DEFINE_FILTER(or),
	{ { 0, NULL }, NULL }
};

static filter_base_t*
filter_parse_object(filter_parse_ctx_t* ctx, json_object_t* obj)
{
	filter_def_t* cur;
	str_t* type;
	
	type = filter_get_json_object_string_value(obj, "type", sizeof("type") - 1);
	if (type == NULL)
	{
		snprintf(ctx->error, ctx->error_size, "filter: missing type field");
		return NULL;
	}
	
	for (cur = filter_defs; ; cur++)
	{
		if (cur->name.len == 0)
		{
			snprintf(ctx->error, ctx->error_size, "filter: invalid filter type");
			return NULL;			
		}
		
		if (type->len == cur->name.len && 
			memcmp(type->data, cur->name.data, type->len) == 0)
		{
			break;
		}
	}
	
	return cur->parse(ctx, obj);
}

filter_base_t*
filter_parse(char* str, char* error, size_t error_size)
{
	filter_parse_ctx_t ctx;
	filter_base_t* result;
	json_value_t json;
	
	error[0] = '\0';

	if (json_parse(NULL, str, &json, error, error_size) != JSON_OK)
	{
		goto error;
	}

	if (json.type != JSON_OBJECT)
	{
		snprintf(error, error_size, "root json element is not an object");
		goto error;
	}
	
	ctx.error = error;
	ctx.error_size = error_size;

	result = filter_parse_object(&ctx, &json.v.obj);
	if (result == NULL)
	{
		goto error;
	}
	
	return result;
	
error:

	error[error_size - 1] = '\0';			// make sure it's null terminated
	return NULL;
}

bool_t
filter_eval(filter_base_t* filter, char* data, size_t len)
{
	str_t str;
	
	str.data = data;
	str.len = len;
	return filter->eval(filter, &str);
}