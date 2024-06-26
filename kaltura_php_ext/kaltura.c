#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "php.h"
#include "php_kaltura.h"
#include "zend_exceptions.h"

#ifndef TSRMLS_D
#define TSRMLS_D void
#define TSRMLS_DC
#define TSRMLS_C
#define TSRMLS_CC
#define TSRMLS_FETCH()
#endif

#if (PHP_VERSION_ID >= 70000)
#include "ext/standard/php_smart_string.h"
zval dummy;
#else
#include "php_smart_str.h"
#endif


#if (PHP_VERSION_ID >= 80000)
#include "php_versions/php_8.h"
#elif (PHP_VERSION_ID >= 70000)
#include "php_versions/php_7.h"
#else
#include "php_versions/php_5.h"
#endif

typedef struct
{
	smart_string buf;
	zend_bool ignore_null;
} serialize_params_t;

static int smart_str_append_double(smart_string* buf, double val)
{
	char temp_buf[MAX_LENGTH_OF_DOUBLE];
	sprintf(temp_buf, "%.*G", (int) EG(precision), val);
	smart_string_appends(buf, temp_buf);
}

ZEND_API zend_class_entry *zend_exception_get_default(TSRMLS_D);

#define smart_str_appendl_fixed(dest, src) \
   	smart_string_appendl_ex((dest), (src), sizeof(src) - 1, 0)

static zend_function_entry kaltura_functions[] = {
#if (PHP_VERSION_ID >= 80000)
    PHP_FE(kaltura_serialize_xml, arginfo_kaltura_serialize_xml)
#else
    PHP_FE(kaltura_serialize_xml, NULL)
#endif
    {NULL, NULL, NULL}
};

static int kaltura_request_startup_func(INIT_FUNC_ARGS);

zend_module_entry kaltura_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
    STANDARD_MODULE_HEADER,
#endif
    PHP_KALTURA_EXTNAME,
    kaltura_functions,
    NULL,			// MINIT
    NULL,			// MSHUTDOWN
    kaltura_request_startup_func,			// RINIT
    NULL,			// RSHUTDOWN
    NULL,			// info
#if ZEND_MODULE_API_NO >= 20010901
    PHP_KALTURA_VERSION,
#endif
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_KALTURA
ZEND_GET_MODULE(kaltura)
#endif

PHPAPI void kaltura_serialize_xml_internal(zval **arg, serialize_params_t* params);

PHP_FUNCTION(kaltura_serialize_xml)
{
	serialize_params_t params = {{0}};
#if (PHP_VERSION_ID >= 70000)
	zval *arg;

	ZEND_PARSE_PARAMETERS_START(2, 2)
		Z_PARAM_ZVAL(arg)
		Z_PARAM_BOOL(params.ignore_null)
	ZEND_PARSE_PARAMETERS_END();

	kaltura_serialize_xml_internal(&arg, &params);
#else
	zval **arg;

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "Zb", &arg, &params.ignore_null) == FAILURE) {
		return;
	}

	kaltura_serialize_xml_internal(arg, &params);
#endif

	if (params.buf.c) {
		#if (PHP_VERSION_ID >= 70000)
			RETVAL_STRINGL(params.buf.c, params.buf.len);
			smart_string_free(&params.buf);
		#else
			RETURN_STRINGL(params.buf.c, params.buf.len, 0);
		#endif
	} else {
		RETURN_NULL();
    }
}

#if (PHP_VERSION_ID >= 70000)
static int kaltura_serialize_xml_exception_args(zval *zv_nptr, zend_ulong index_key, zend_string *hash_key, serialize_params_t* params)
{
	zval **zv = &zv_nptr;
#else
static int kaltura_serialize_xml_exception_args(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
#endif
	if(is_numeric_key(hash_key, zv))
		return ZEND_HASH_APPLY_KEEP;

	#if (PHP_VERSION_ID < 70000)
	serialize_params_t* params;
	params = va_arg(args, serialize_params_t*);
	#endif

	smart_str_appendl_fixed(&params->buf, "<item><objectType>KalturaApiExceptionArg</objectType><name>");
	smart_string_append_key(&params->buf, hash_key);
	smart_str_appendl_fixed(&params->buf, "</name><value>");
	write_string_xml_encoded(&params->buf, Z_STRVAL_P(*zv));
	smart_str_appendl_fixed(&params->buf, "</value></item>");

	return ZEND_HASH_APPLY_KEEP;
}

#if (PHP_VERSION_ID >= 70000)
static int kaltura_serialize_xml_array_element(zval *zv_nptr, void *argument TSRMLS_DC)
{
	zval **zv = &zv_nptr;
#else
static int kaltura_serialize_xml_array_element(zval **zv, void *argument TSRMLS_DC)
{
#endif
	serialize_params_t* params = (serialize_params_t*)argument;

	smart_str_appendl_fixed(&params->buf, "<item>");
	kaltura_serialize_xml_internal(zv, params);
	smart_str_appendl_fixed(&params->buf, "</item>");
	return ZEND_HASH_APPLY_KEEP;
}

#if (PHP_VERSION_ID >= 70000)
static int kaltura_serialize_xml_map_element(zval *zv_nptr, zend_ulong index_key, zend_string *hash_key, serialize_params_t* params)
{
	zval **zv = &zv_nptr;
#else
static int kaltura_serialize_xml_map_element(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
	serialize_params_t* params = va_arg(args, serialize_params_t*);
	zend_ulong index_key = hash_key->h;
#endif

	smart_str_appendl_fixed(&params->buf, "<item><itemKey>");
	smart_string_append_item_key(&params->buf, hash_key, index_key);
	smart_str_appendl_fixed(&params->buf, "</itemKey>");
	kaltura_serialize_xml_internal(zv, params);
	smart_str_appendl_fixed(&params->buf, "</item>");
	return ZEND_HASH_APPLY_KEEP;
}

#if (PHP_VERSION_ID >= 70000)
static int kaltura_serialize_xml_object_property(zval *zv_nptr, zend_ulong index_key, zend_string *hash_key, serialize_params_t* params)
{
	zval **zv = &zv_nptr;
	const char *prop_name, *class_name;
#else
static int kaltura_serialize_xml_object_property(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
	char *prop_name, *class_name;
	serialize_params_t* params = va_arg(args, serialize_params_t*);
#endif

	size_t prop_len;
	int mangled;

#if (PHP_VERSION_ID >= 70000)
	if (hash_key == NULL)
#else
	if (hash_key->nKeyLength == 0)
#endif
		return ZEND_HASH_APPLY_KEEP;		// not a string key

	if (params->ignore_null && Z_TYPE_P(*zv) == IS_NULL)
		return ZEND_HASH_APPLY_KEEP;		// null property

#if (PHP_VERSION_ID >= 70000)
	mangled = zend_unmangle_property_name_ex(hash_key, &class_name, &prop_name, &prop_len);
#else
	mangled = zend_unmangle_property_name(hash_key->arKey, hash_key->nKeyLength - 1, &class_name, &prop_name);
	prop_len = strlen(prop_name);
#endif
	if (class_name && mangled == SUCCESS)
		return ZEND_HASH_APPLY_KEEP;		// private or protected (class_name == '*')

	smart_string_appendc(&params->buf, '<');
	smart_string_appendl(&params->buf, prop_name, prop_len);
	smart_string_appendc(&params->buf, '>');

	kaltura_serialize_xml_internal(zv, params);

	smart_str_appendl_fixed(&params->buf, "</");
	smart_string_appendl(&params->buf, prop_name, prop_len);
	smart_string_appendc(&params->buf, '>');

	return ZEND_HASH_APPLY_KEEP;
}

const char* xml_encode_table[256] = {
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	"&quot;",
	NULL,
	NULL,
	NULL,
	"&amp;",
	"&apos;",
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	"&lt;",
	NULL,
	"&gt;",
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
};

static void write_string_xml_encoded(smart_string* buf, const char* str)
{
	const char* chunkStart = str;
	const char* escaped;
	int chunkSize;

	for (; *str; str++)
	{
		escaped = xml_encode_table[(unsigned char)*str];
		if (escaped)
		{
			chunkSize = str - chunkStart;
			smart_string_appendl(buf, chunkStart, chunkSize);
			chunkStart = str + 1;
			smart_string_appends(buf, escaped);
		}
	}
	chunkSize = str - chunkStart;
	smart_string_appendl(buf, chunkStart, chunkSize);
}

static int kaltura_request_startup_func(INIT_FUNC_ARGS)
{
	kaltura_typed_array_ce = NULL;
	kaltura_associative_array_ce = NULL;
}

PHPAPI void kaltura_serialize_xml_internal(zval **arg, serialize_params_t* params)
{
	HashTable *myht;

#if PHP_VERSION_ID >= 70000
	zend_string *class_name;
	zend_ulong num;
	zend_ulong class_name_len;
	zend_string *key;
	zval *val;
#else
	#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION > 3) || (PHP_MAJOR_VERSION > 5)
	const char *class_name;
	#else
	char *class_name;
	#endif
	zend_uint class_name_len;
#endif

	switch (Z_TYPE_P(*arg)) {
	#if (PHP_VERSION_ID >= 70000)
		case IS_TRUE:
			smart_string_appendc(&params->buf, '1');
			break;
		case IS_FALSE:
			smart_string_appendc(&params->buf, '0');
			break;
	#else
		case IS_BOOL:

			if (Z_LVAL_P(*arg))
				smart_string_appendc(&params->buf, '1');
			else
				smart_string_appendc(&params->buf, '0');
			break;
	#endif
		case IS_NULL:
			break;

		case IS_LONG:
			smart_string_append_long(&params->buf, Z_LVAL_P(*arg));
			break;

		case IS_DOUBLE:
			smart_str_append_double(&params->buf, Z_DVAL_P(*arg));
			break;

		case IS_STRING:
        	write_string_xml_encoded(&params->buf, Z_STRVAL_P(*arg));
			break;

		case IS_ARRAY:
			myht = Z_ARRVAL_P(*arg);
		#if PHP_VERSION_ID >= 70000
			ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
				kaltura_serialize_xml_array_element(val, params);
			} ZEND_HASH_FOREACH_END();
		#else
			zend_hash_apply_with_argument(myht TSRMLS_CC, (apply_func_arg_t) kaltura_serialize_xml_array_element, params);
		#endif
			break;

		case IS_OBJECT:
			// handle KalturaAssociativeArray
			if (kaltura_associative_array_ce == NULL)
				#if PHP_VERSION_ID >= 70000
					kaltura_associative_array_ce = zend_hash_str_find_ptr(EG(class_table), "kalturaassociativearray", sizeof("kalturaassociativearray") - 1);
				if (kaltura_associative_array_ce != NULL && instanceof_function(Z_OBJCE_P(*arg), kaltura_associative_array_ce))
				#else
					zend_hash_find(EG(class_table), "kalturaassociativearray", sizeof("kalturaassociativearray"), (void **) &kaltura_associative_array_ce);
				if (kaltura_associative_array_ce != NULL && instanceof_function(Z_OBJCE_P(*arg), *kaltura_associative_array_ce))
				#endif
			{
				zval *arr;

				#if PHP_VERSION_ID >= 70000
					arr = zend_read_property_wrapper(kaltura_associative_array_ce, *arg, "array", sizeof("array") - 1, 0);
					if (Z_ISREF_P(arr)) {
						ZVAL_UNREF(arr);
					}
					myht = Z_ARRVAL_P(arr);
					ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
						kaltura_serialize_xml_map_element(val,num,key,params);
					} ZEND_HASH_FOREACH_END();
				#else
					arr = zend_read_property_wrapper(*kaltura_associative_array_ce, *arg, "array", sizeof("array") - 1, 0);
					myht = Z_ARRVAL_P(arr);
					zend_hash_apply_with_arguments(myht TSRMLS_CC, (apply_func_args_t) kaltura_serialize_xml_map_element, 1, params);
				#endif

				break;
			}

			// handle KalturaTypedArray
			if (kaltura_typed_array_ce == NULL)
				#if PHP_VERSION_ID >= 70000
					kaltura_typed_array_ce = zend_hash_str_find_ptr(EG(class_table), "kalturatypedarray", sizeof("kalturatypedarray") - 1);
				if (kaltura_typed_array_ce != NULL && instanceof_function(Z_OBJCE_P(*arg), kaltura_typed_array_ce))
				#else
					zend_hash_find(EG(class_table), "kalturatypedarray", sizeof("kalturatypedarray"), (void **) &kaltura_typed_array_ce);
				if (kaltura_typed_array_ce != NULL && instanceof_function(Z_OBJCE_P(*arg), *kaltura_typed_array_ce))
				#endif
			{
				zval *arr;

				#if PHP_VERSION_ID >= 70000
					arr = zend_read_property_wrapper(kaltura_typed_array_ce, *arg, "array", sizeof("array") - 1, 0);
					if (Z_ISREF_P(arr)) {
						ZVAL_UNREF(arr);
					}
					myht = Z_ARRVAL_P(arr);
					ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
						kaltura_serialize_xml_array_element(val,params);
					} ZEND_HASH_FOREACH_END();
				#else
					arr = zend_read_property_wrapper(*kaltura_typed_array_ce, *arg, "array", sizeof("array") - 1, 0);
					myht = Z_ARRVAL_P(arr);
					zend_hash_apply_with_argument(myht TSRMLS_CC, (apply_func_arg_t) kaltura_serialize_xml_array_element, params);
				#endif
				break;
			}

			// get the class name
			if (!Z_OBJ_HANDLER(**arg, get_class_name))
			{
				zend_throw_exception(zend_exception_get_default(TSRMLS_C), "Failed to get class name", 0 TSRMLS_CC);
				break;
			}

		#if PHP_VERSION_ID >= 70000
			class_name = Z_OBJ_HANDLER(**arg, get_class_name)(Z_OBJ_P(*arg));
			class_name_len = class_name->len;
		#else
			Z_OBJ_HANDLER(**arg, get_class_name)(*arg, &class_name, &class_name_len, 0 TSRMLS_CC);
		#endif
			if (instanceof_function(Z_OBJCE_P(*arg), zend_exception_get_default(TSRMLS_C)))
			{
				// exceptions
				zval *prop;

				smart_str_appendl_fixed(&params->buf, "<error><objectType>");
				smart_string_append_class_name(&params->buf, class_name, class_name_len);
				smart_str_appendl_fixed(&params->buf, "</objectType><code>");
				prop = zend_read_property_wrapper(zend_exception_get_default(TSRMLS_C), *arg, "code", sizeof("code") - 1, 0);

				kaltura_serialize_xml_internal(&prop, params);
				smart_str_appendl_fixed(&params->buf, "</code><message>");
				prop = zend_read_property_wrapper(zend_exception_get_default(TSRMLS_C), *arg, "message", sizeof("message") - 1, 0);

				kaltura_serialize_xml_internal(&prop, params);
				smart_str_appendl_fixed(&params->buf, "</message>");

				prop = zend_read_property_wrapper(Z_OBJCE_P(*arg), *arg, "args", sizeof("args") - 1, 1);		// if the property is missing, ignore silently
				if (prop != NULL && Z_TYPE_P(prop) == IS_ARRAY)
				{
					myht = Z_ARRVAL_P(prop);

					smart_str_appendl_fixed(&params->buf, "<args>");
				#if PHP_VERSION_ID >= 70000
					ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
						kaltura_serialize_xml_exception_args(val,num,key,params);
					}  ZEND_HASH_FOREACH_END();
				#else
					zend_hash_apply_with_arguments(myht TSRMLS_CC, (apply_func_args_t) kaltura_serialize_xml_exception_args, 1, params);
				#endif
					smart_str_appendl_fixed(&params->buf, "</args>");
				}
				smart_str_appendl_fixed(&params->buf, "</error>");
			}
			else
			{
				// other objects
				smart_str_appendl_fixed(&params->buf, "<objectType>");
				smart_string_append_class_name(&params->buf, class_name, class_name_len);
				smart_str_appendl_fixed(&params->buf, "</objectType>");

				myht = Z_OBJPROP_P(*arg);

				#if PHP_VERSION_ID >= 70000
				ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
					kaltura_serialize_xml_object_property(val, num, key, params);
				} ZEND_HASH_FOREACH_END();
				#else
				zend_hash_apply_with_arguments(myht TSRMLS_CC, (apply_func_args_t) kaltura_serialize_xml_object_property, 1, params);
				#endif
			}

			release_class_name(class_name);
			break;

		case IS_RESOURCE:
			zend_throw_exception(zend_exception_get_default(TSRMLS_C), "The type [resource] cannot be serialized", 0 TSRMLS_CC);
			break;

		default:
			zend_throw_exception(zend_exception_get_default(TSRMLS_C), "The type [unknown type] cannot be serialized", 0 TSRMLS_CC);
			break;
	}
}

