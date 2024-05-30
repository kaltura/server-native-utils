
#define zend_read_property_wrapper(scope, object, name, name_length, silent) \
                zend_read_property(scope, object, name, name_length, silent, &dummy)

#define smart_string_append_key(buf, hash_key) \
                                smart_string_appendl(buf, hash_key->val, hash_key->len)

#define smart_string_append_class_name(buf, class_name, class_name_len) \
                                smart_string_appendl(buf, class_name->val, class_name_len)

#define smart_string_append_item_key(buf, hash_key, index_key) { \
                                        if (hash_key != NULL)   { smart_string_appendl(buf, hash_key->val, hash_key->len); } \
                                        else { smart_string_append_long(buf, index_key); } \
                                }

#define is_numeric_key(hash_key, zv) \
                                (hash_key == NULL || Z_TYPE_P(*zv) != IS_STRING) //not a string key || not a string value

#define release_class_name(s) zend_string_release(s)

static void write_string_xml_encoded(smart_string* buf, const char* str);
static zend_class_entry *kaltura_typed_array_ce = NULL;
static zend_class_entry *kaltura_associative_array_ce = NULL;