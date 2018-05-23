
#define zend_read_property_wrapper(scope, object, name, name_length, silent) \
                zend_read_property(scope, object, name, name_length, silent TSRMLS_CC)

#define smart_string_append_key(buf, hash_key) \
                                smart_string_appendl(buf, hash_key->arKey, hash_key->nKeyLength - 1)

#define smart_string_append_class_name(buf, class_name, class_name_len) \
                                smart_string_appendl(buf, class_name, class_name_len)

#define smart_string_append_item_key(buf, hash_key, index_key) { \
                                        if (hash_key->nKeyLength > 0) { smart_string_appendl(buf, hash_key->arKey, hash_key->nKeyLength - 1); } \
                                        else { smart_string_append_long(buf, index_key); } \
                                }

#define is_numeric_key(hash_key, zv) \
                                (hash_key->nKeyLength == 0 || Z_TYPE_P(*zv) != IS_STRING) //not a string key || not a string value

#define release_class_name(s) efree((char*)s)

#define smart_string_appends smart_str_appends
#define smart_string_appendc smart_str_appendc
#define smart_string_appendl smart_str_appendl
#define smart_string_append_long smart_str_append_long
#define smart_string_appendl_ex smart_str_appendl_ex

static void write_string_xml_encoded(smart_str* buf, const char* str);
static zend_class_entry **kaltura_typed_array_ce = NULL;
static zend_class_entry **kaltura_associative_array_ce = NULL;
typedef smart_str smart_string;