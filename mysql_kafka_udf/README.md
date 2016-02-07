# MySQL Kafka producer UDF

## Installation

```
apt-get install g++ gcc git zlib1g-dev make libmysqlclient-dev
 
mkdir /opt/kaltura
cd /opt/kaltura
git clone https://github.com/kaltura/server-native-utils
cd server-native-utils/mysql_kafka_udf/
 
git clone https://github.com/edenhill/librdkafka
cd librdkafka/
./configure
make install
 
bash build.sh
cp mysql_kafka_udf.so /usr/lib/mysql/plugin/
mysql < sql/install_functions.sql
```

## Functions

### kafka_conf_set

Sets a global configuration parameter.

Parameters:
* string key - the configuration parameter name, see the librdkafka documentation for the list of possible keys
* string value - the configuration value

Return value: 1 if successful, 0 otherwise

### kafka_topic_conf_set

Sets a topic configuration parameter.

Parameters:
* string topic - the topic name
* string key - the configuration parameter name, see the librdkafka documentation for the list of possible keys
* string value - the configuration value

Return value: 1 if successful, 0 otherwise

### kafka_produce

Sends a single message to a broker.

Parameters:
* string topic - the topic name
* int partition - the target partition, pass -1 for automatic partitioning
* string payload - the message payload
* optional string key - the message key

Return value: 1 if successful, 0 otherwise

### kafka_reload

Applies any global/topic configuration changes.

Parameters: N/A

Return value: 1 (always succeeds)

## Copyright & License

All code in this project is released under the [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html) unless a different license for a particular library is specified in the applicable library path. 

Copyright © Kaltura Inc. All rights reserved.
