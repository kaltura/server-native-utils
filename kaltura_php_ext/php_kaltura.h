#ifndef PHP_KALTURA_H
#define PHP_KALTURA_H 1
#define PHP_KALTURA_VERSION "1.0"
#define PHP_KALTURA_EXTNAME "kaltura"

PHP_FUNCTION(kaltura_serialize_xml);

extern zend_module_entry kaltura_module_entry;
#define phpext_kaltura_ptr &kaltura_module_entry

#endif
