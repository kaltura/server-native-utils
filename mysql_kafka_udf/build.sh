gcc -shared -o mysql_kafka_udf.so mysql_kafka_udf.c hash.c -I/usr/include/mysql/ -fPIC -lrdkafka -L/usr/local/lib
