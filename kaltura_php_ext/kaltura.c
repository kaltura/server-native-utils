#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include "php.h"
#include "php_kaltura.h"
#include "zend_exceptions.h"

#if (PHP_VERSION_ID < 70000)
#include "php_smart_str.h"
#else
#include "ext/standard/php_smart_string.h"
#endif


#if (PHP_VERSION_ID >= 70000)
#define smart_str_appendl_fixed(dest, src) \
   	smart_string_appendl((dest), (src), sizeof(src) - 1);
#else
#define smart_str_appendl_fixed(dest, src) \
	smart_str_appendl_ex((dest), (src), sizeof(src) - 1, 0);
#endif

ZEND_API zend_class_entry *zend_exception_get_default(TSRMLS_D);

#if (PHP_VERSION_ID >= 70000)
static void write_string_xml_encoded(smart_string* buf, const char* str);
#else
static void write_string_xml_encoded(smart_str* buf, const char* str);
#endif

#if (PHP_VERSION_ID >= 70000)
static zend_class_entry *kaltura_typed_array_ce = NULL;
static zend_class_entry *kaltura_associative_array_ce = NULL;
#else
static zend_class_entry **kaltura_typed_array_ce = NULL;
static zend_class_entry **kaltura_associative_array_ce = NULL;
#endif

static zend_function_entry kaltura_functions[] = {
    PHP_FE(kaltura_serialize_xml, NULL)
    {NULL, NULL, NULL}
};

typedef struct
{
#if (PHP_VERSION_ID >= 70000)
	smart_string buf;
#else
	smart_str buf;
#endif
	int ignore_null;
} serialize_params_t;

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

	if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "zb", &arg, &params.ignore_null) == FAILURE) {
		return;
	}

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
			RETURN_STRINGL(params.buf.c, params.buf.len);
		#else
			RETURN_STRINGL(params.buf.c, params.buf.len, 0);
		#endif
		} else {
			RETURN_NULL();
        }
}

static int kaltura_serialize_xml_exception_args(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
	serialize_params_t* params;
#if (PHP_VERSION_ID >= 70000)
	if (hash_key->key == NULL || Z_TYPE_P(*zv) != IS_STRING) // not a string key || not a string value
#else
	if (hash_key->nKeyLength == 0 || Z_TYPE_P(*zv) != IS_STRING) // not a string key || not a string value
#endif
		return ZEND_HASH_APPLY_KEEP;
		
	params = va_arg(args, serialize_params_t*);
	
	smart_str_appendl_fixed(&params->buf, "<item><objectType>KalturaApiExceptionArg</objectType><name>");
#if (PHP_VERSION_ID >= 70000)
	smart_string_appendl(&params->buf, hash_key->key, hash_key->key->len - 1);
#else
	smart_str_appendl(&params->buf, hash_key->arKey, hash_key->nKeyLength - 1);
#endif
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

static int kaltura_serialize_xml_map_element(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
	serialize_params_t* params = va_arg(args, serialize_params_t*);
	
	smart_str_appendl_fixed(&params->buf, "<item><itemKey>");

#if (PHP_VERSION_ID >= 70000)
	if (hash_key->key != NULL)
    {		
    	smart_string_appendl(&params->buf, hash_key->key, hash_key->key->len - 1);
    }
    else
    {
    	smart_string_append_long(&params->buf, hash_key->h);
    }
#else
	if (hash_key->nKeyLength > 0)
	{		
		smart_str_appendl(&params->buf, hash_key->arKey, hash_key->nKeyLength - 1);
	}
	else
	{
		smart_str_append_long(&params->buf, hash_key->h);
	}
#endif
	smart_str_appendl_fixed(&params->buf, "</itemKey>");
	kaltura_serialize_xml_internal(zv, params);
	smart_str_appendl_fixed(&params->buf, "</item>");
	return ZEND_HASH_APPLY_KEEP;
}

static int kaltura_serialize_xml_object_property(zval **zv TSRMLS_DC, int num_args, va_list args, zend_hash_key *hash_key)
{
#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION <= 3) || (PHP_MAJOR_VERSION < 5)
	char *prop_name, *class_name;
#else
	const char *prop_name, *class_name;
#endif
	serialize_params_t* params = va_arg(args, serialize_params_t*);
	int prop_name_len = 0;
	int mangled;

#if (PHP_VERSION_ID >= 70000)
	if (hash_key->key == NULL)
#else
	if (hash_key->nKeyLength == 0)
#endif
		return ZEND_HASH_APPLY_KEEP;		// not a string key
		
	if (params->ignore_null && Z_TYPE_P(*zv) == IS_NULL)
		return ZEND_HASH_APPLY_KEEP;		// null property

#if (PHP_VERSION_ID >= 70000)
	size_t key_len;
	mangled = zend_unmangle_property_name_ex(hash_key->key, &class_name, &prop_name, &key_len);
#else
	#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION <= 3) || (PHP_MAJOR_VERSION < 5)
	mangled = zend_unmangle_property_name(hash_key->arKey, hash_key->nKeyLength - 1, &class_name, &prop_name);
	#else
	mangled = zend_unmangle_property_name_ex(hash_key->arKey, hash_key->nKeyLength - 1, &class_name, &prop_name, &prop_name_len);
	#endif
#endif
	if (class_name && mangled == SUCCESS)
		return ZEND_HASH_APPLY_KEEP;		// private or protected (class_name == '*')
 
	prop_name_len = strlen(prop_name);

#if (PHP_VERSION_ID >= 70000)
	smart_string_appendc(&params->buf, '<');
	smart_string_appendl(&params->buf, prop_name, prop_name_len);
	smart_string_appendc(&params->buf, '>');
#else
	smart_str_appendc(&params->buf, '<');
	smart_str_appendl(&params->buf, prop_name, prop_name_len);
	smart_str_appendc(&params->buf, '>');
#endif

	kaltura_serialize_xml_internal(zv, params);

#if (PHP_VERSION_ID >= 70000)
	smart_str_appendl_fixed(&params->buf, "</");
	smart_string_appendl(&params->buf, prop_name, prop_name_len);
	smart_string_appendc(&params->buf, '>');
#else
	smart_str_appendl_fixed(&params->buf, "</");
    smart_str_appendl(&params->buf, prop_name, prop_name_len);
    smart_str_appendc(&params->buf, '>');
#endif

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

#if (PHP_VERSION_ID >= 70000)
static void write_string_xml_encoded(smart_string* buf, const char* str)
#else
static void write_string_xml_encoded(smart_str* buf, const char* str)
#endif
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
		#if (PHP_VERSION_ID >= 70000)
			smart_string_appendl(buf, chunkStart, chunkSize);
		#else
			smart_str_appendl(buf, chunkStart, chunkSize);
		#endif
			chunkStart = str + 1;
		#if (PHP_VERSION_ID >= 70000)
			smart_string_appends(buf, escaped);
		#else
			smart_str_appends(buf, escaped);
		#endif
		}	
	}
	chunkSize = str - chunkStart;
#if (PHP_VERSION_ID >= 70000)
	smart_string_appendl(buf, chunkStart, chunkSize);
#else
	smart_str_appendl(buf, chunkStart, chunkSize);
#endif
}

static int kaltura_request_startup_func(INIT_FUNC_ARGS)
{
	kaltura_typed_array_ce = NULL;
	kaltura_associative_array_ce = NULL;
}

#if (PHP_VERSION_ID >= 70000)
static int smart_str_append_double(smart_string* buf, double val)
#else
static int smart_str_append_double(smart_str* buf, double val)
#endif
{
	char temp_buf[MAX_LENGTH_OF_DOUBLE];
	sprintf(temp_buf, "%.*G", (int) EG(precision), val);
#if (PHP_VERSION_ID >= 70000)
	smart_string_appends(buf, temp_buf);
#else
	smart_str_appends(buf, temp_buf);
#endif
}

PHPAPI void kaltura_serialize_xml_internal(zval **arg, serialize_params_t* params) 
{
	HashTable *myht;
#if PHP_VERSION_ID >= 70000
	zend_string *class_name;
	zend_ulong num;
	zend_string *key;
	zval *val;
#else
	#if (PHP_MAJOR_VERSION == 5 && PHP_MINOR_VERSION > 3) || (PHP_MAJOR_VERSION > 5)
	const char *class_name;
	#else
	char *class_name;
	#endif
#endif

#if (PHP_VERSION_ID >= 70000)
	zval dummy;
#else
	zend_uint class_name_len;
#endif

	switch (Z_TYPE_P(*arg)) {
	#if (PHP_VERSION_ID >= 70000)
		case IS_TRUE:
		case IS_FALSE:
	#else
		case IS_BOOL:
	#endif
			if (Z_LVAL_P(*arg))
			#if (PHP_VERSION_ID >= 70000)
            	smart_string_appendc(&params->buf, '1');
            #else
            	smart_str_appendc(&params->buf, '1');
            #endif
			else
			#if (PHP_VERSION_ID >= 70000)
				smart_string_appendc(&params->buf, '0');
			#else
				smart_str_appendc(&params->buf, '0');
			#endif
			break;

		case IS_NULL:
			break;
			
		case IS_LONG:
		#if (PHP_VERSION_ID >= 70000)
			smart_string_append_long(&params->buf, Z_LVAL_P(*arg));
		#else
			smart_str_append_long(&params->buf, Z_LVAL_P(*arg));
		#endif
			break;

		case IS_DOUBLE:
			smart_str_append_double(&params->buf, Z_DVAL_P(*arg));
			break;
	
		case IS_STRING:
        	write_string_xml_encoded(&params->buf, Z_STRVAL_P(*arg));
			break;
	
		case IS_ARRAY:
			myht = Z_ARRVAL_P(*arg);
			zend_hash_apply_with_argument(myht TSRMLS_CC, (apply_func_arg_t) kaltura_serialize_xml_array_element, params);
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
					arr = zend_read_property(kaltura_associative_array_ce, *arg, "array", sizeof("array") - 1, 0, &dummy);
				#else
					arr = zend_read_property(*kaltura_associative_array_ce, *arg, "array", sizeof("array") - 1, 0 TSRMLS_CC);
				#endif
				myht = Z_ARRVAL_P(arr);
			#if PHP_VERSION_ID >= 70000
				ZEND_HASH_INC_APPLY_COUNT(myht);
				ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
					kaltura_serialize_xml_array_element(val,params);
				} ZEND_HASH_FOREACH_END();
				ZEND_HASH_DEC_APPLY_COUNT(myht);
			#else
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
					arr = zend_read_property(kaltura_typed_array_ce, *arg, "array", sizeof("array") - 1, 0, &dummy);
				#else
					arr = zend_read_property(*kaltura_typed_array_ce, *arg, "array", sizeof("array") - 1, 0 TSRMLS_CC);
				#endif
				myht = Z_ARRVAL_P(arr);
				
			#if PHP_VERSION_ID >= 70000
				ZEND_HASH_INC_APPLY_COUNT(myht);
				ZEND_HASH_FOREACH_KEY_VAL_IND(myht, num, key, val) {
					kaltura_serialize_xml_array_element(val,params);
				} ZEND_HASH_FOREACH_END();
				ZEND_HASH_DEC_APPLY_COUNT(myht);
			#else
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
			class_name=Z_OBJ_HANDLER(**arg, get_class_name)(Z_OBJ_P(*arg));
		#else
			Z_OBJ_HANDLER(**arg, get_class_name)(*arg, &class_name, &class_name_len, 0 TSRMLS_CC);
		#endif
		
			if (instanceof_function(Z_OBJCE_P(*arg), zend_exception_get_default(TSRMLS_C)))
			{
				// exceptions
				zval *prop;
			
				smart_str_appendl_fixed(&params->buf, "<error><objectType>");
			#if PHP_VERSION_ID >= 70000
				smart_string_appendl(&params->buf, class_name->val, class_name->len);
			#else
				smart_str_appendl(&params->buf, class_name, class_name_len);
			#endif
				smart_str_appendl_fixed(&params->buf, "</objectType><code>");
				#if PHP_VERSION_ID >= 70000
					prop = zend_read_property(zend_exception_get_default(TSRMLS_C), *arg, "code", sizeof("code") - 1, 0, &dummy);
				#else
					prop = zend_read_property(zend_exception_get_default(TSRMLS_C), *arg, "code", sizeof("code") - 1, 0 TSRMLS_CC);
				#endif
				kaltura_serialize_xml_internal(&prop, params);
				smart_str_appendl_fixed(&params->buf, "</code><message>");
				#if PHP_VERSION_ID >= 70000
					prop = zend_read_property(zend_exception_get_default(TSRMLS_C), *arg, "message", sizeof("message") - 1, 0, &dummy);
				#else
					prop = zend_read_property(zend_exception_get_default(TSRMLS_C), *arg, "message", sizeof("message") - 1, 0 TSRMLS_CC);
				#endif
				kaltura_serialize_xml_internal(&prop, params);
				smart_str_appendl_fixed(&params->buf, "</message>");
				
				#if PHP_VERSION_ID >= 70000
					prop = zend_read_property(Z_OBJCE_P(*arg), *arg, "args", sizeof("args") - 1, 1, &dummy);		// if the property is missing, ignore silently
				#else
					prop = zend_read_property(Z_OBJCE_P(*arg), *arg, "args", sizeof("args") - 1, 1 TSRMLS_CC);		// if the property is missing, ignore silently
				#endif
				if (prop != NULL && Z_TYPE_P(prop) == IS_ARRAY)
				{
					myht = Z_ARRVAL_P(prop);

					smart_str_appendl_fixed(&params->buf, "<args>");
					zend_hash_apply_with_arguments(myht TSRMLS_CC, (apply_func_args_t) kaltura_serialize_xml_exception_args, 1, params);
					smart_str_appendl_fixed(&params->buf, "</args>");
				}
				smart_str_appendl_fixed(&params->buf, "</error>");
			}
			else
			{
				// other objects				
				smart_str_appendl_fixed(&params->buf, "<objectType>");
			#if PHP_VERSION_ID >= 70000
				smart_string_appendl(&params->buf, class_name->val, class_name->len);
			#else
				smart_str_appendl(&params->buf, class_name, class_name_len);
			#endif
				smart_str_appendl_fixed(&params->buf, "</objectType>");
				
				myht = Z_OBJPROP_P(*arg);
				zend_hash_apply_with_arguments(myht TSRMLS_CC, (apply_func_args_t) kaltura_serialize_xml_object_property, 1, params);
			}
			
			#if PHP_VERSION_ID < 70000
			efree((char*)class_name);
			#else
			zend_string_release(class_name);
			#endif

			break;

		case IS_RESOURCE:
			zend_throw_exception(zend_exception_get_default(TSRMLS_C), "The type [resource] cannot be serialized", 0 TSRMLS_CC);
			break;
			
		default:
			zend_throw_exception(zend_exception_get_default(TSRMLS_C), "The type [unknown type] cannot be serialized", 0 TSRMLS_CC);
			break;
	}
}
