gcc -DSTANDARD -DHAVE_DLOPEN -shared -g -O3 -o mysql_memcached_async.so mysql_memcached_async.c itp.c buffer_pool.c -I/usr/include/mysql/ -fPIC -L/usr/local/lib
