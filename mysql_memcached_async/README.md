# MySQL Asynchronous Memcache set UDF

## Installation

```
apt-get install g++ gcc git make libmysqlclient-dev
 
mkdir /opt/kaltura
cd /opt/kaltura
git clone https://github.com/kaltura/server-native-utils
cd server-native-utils/mysql_memcached_async/
bash build.sh
cp mysql_memcached_async.so /usr/lib/mysql/plugin/
mysql < sql/install_functions.sql
```

## Functions

### memc_async_setup

Sets up the UDF.

Parameters:
* string ip - the memcache ip address
* int port - the memcache port

Return value: 1 if successful, 0 otherwise

### memc_async_set

Sets a value in memcache.

Parameters:
* string key - the key name.
* string value - the key value.
* int expiration - the expiration time in seconds.

Return value: 1 if successful, 0 otherwise

## Copyright & License

All code in this project is released under the [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html) unless a different license for a particular library is specified in the applicable library path. 

Copyright © Kaltura Inc. All rights reserved.
