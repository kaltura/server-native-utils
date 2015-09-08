DROP FUNCTION IF EXISTS kafka_conf_set;
DROP FUNCTION IF EXISTS kafka_brokers_add;
DROP FUNCTION IF EXISTS kafka_topic_conf_set;
DROP FUNCTION IF EXISTS kafka_produce;

CREATE FUNCTION kafka_conf_set RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_brokers_add RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_topic_conf_set RETURNS INT SONAME "mysql_kafka_udf.so";
CREATE FUNCTION kafka_produce RETURNS INT SONAME "mysql_kafka_udf.so";
