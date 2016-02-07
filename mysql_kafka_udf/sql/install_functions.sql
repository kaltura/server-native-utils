DROP FUNCTION IF EXISTS kafka_conf_set;
DROP FUNCTION IF EXISTS kafka_topic_conf_set;
DROP FUNCTION IF EXISTS kafka_produce;
DROP FUNCTION IF EXISTS kafka_reload;

CREATE FUNCTION kafka_conf_set RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_topic_conf_set RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_produce RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_reload RETURNS INT SONAME "mysql_kafka_udf.so";
