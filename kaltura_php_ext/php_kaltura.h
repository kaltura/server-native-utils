#ifndef PHP_KALTURA_H
#define PHP_KALTURA_H 1
#define PHP_KALTURA_VERSION "1.0"
#define PHP_KALTURA_EXTNAME "kaltura"

ZEND_BEGIN_ARG_INFO_EX(arginfo_kaltura_serialize_xml, 0, 0, 2)
 ZEND_ARG_INFO(0, firstArg)
 ZEND_ARG_OBJ_INFO(0, object)
 ZEND_ARG_OBJ_INFO(0, ignoreNull)
ZEND_END_ARG_INFO()

PHP_FUNCTION(kaltura_serialize_xml);

extern zend_module_entry kaltura_module_entry;
#define phpext_kaltura_ptr &kaltura_module_entry

#endif
