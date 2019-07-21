DROP FUNCTION IF EXISTS memc_async_setup;
DROP FUNCTION IF EXISTS memc_async_set;

CREATE FUNCTION memc_async_setup RETURNS INT SONAME "mysql_memcached_async.so";
CREATE FUNCTION memc_async_set   RETURNS INT SONAME "mysql_memcached_async.so";
