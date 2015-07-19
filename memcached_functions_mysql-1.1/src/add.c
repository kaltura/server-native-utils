/* Copyright (C) 2008 Patrick Galbraith, Brian Aker

  See COPYING file found with distribution for license.

*/

#include <mysql.h>
#include <string.h>

#include <stdio.h>

#include "common.h"

my_bool memc_add_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
long long memc_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void memc_add_deinit(UDF_INIT *initid);
my_bool memc_add_by_key_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
long long memc_add_by_key(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
void memc_add_by_key_deinit(UDF_INIT *initid);

my_bool memc_add_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  memcached_return rc;
  memc_function_st *container;

  container= prepare_args(args, message, MEMC_ADD, 2, 3);
  if (container == NULL)
    return 1;

  /* Init the memcached_st we will use for this pass */
  rc= memc_get_servers(args, &container->memc);

  initid->ptr= (char *)container;

  return (rc == MEMCACHED_SUCCESS ? 0 : 1);
}

long long memc_add(UDF_INIT *initid, UDF_ARGS *args,
                   __attribute__ ((unused)) char *is_null,
                   char *error)
{
  memcached_return rc;

  memc_function_st *container= (memc_function_st *)initid->ptr;

  fix_null_args(args, 2);

  rc= memcached_add(container->memc,
                    args->args[0], (size_t)args->lengths[0],
                    args->args[1], (size_t)args->lengths[1],
                    container->expiration, (uint16_t)0);
  if (rc != MEMCACHED_SUCCESS)
  {
    log_error(args, "memcached_add failed, rc=%d\n", rc);
    return (long long)0;  
  }

  return (long long) 1;
}

void memc_add_deinit(UDF_INIT *initid)
{
  /* if we allocated initid->ptr, free it here */
  memc_function_st *container= (memc_function_st *)initid->ptr;

  memc_free_servers(container->memc);
  free(container);
}

my_bool memc_add_by_key_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
  memcached_return rc;
  memc_function_st *container;

  container= prepare_args(args, message, MEMC_ADD_BY_KEY, 3, 4);
  if (container == NULL)
    return 1;

  /* Init the memcached_st we will use for this pass */
  rc= memc_get_servers(args, &container->memc);

  initid->ptr= (char *)container;

  return (rc == MEMCACHED_SUCCESS ? 0 : 1);
}

long long memc_add_by_key(UDF_INIT *initid, UDF_ARGS *args,
                   __attribute__ ((unused)) char *is_null,
                   char *error)
{
  memcached_return rc;
  memc_function_st *container= (memc_function_st *)initid->ptr;

  fix_null_args(args, 3);

  rc= memcached_add_by_key(container->memc,
                           args->args[0], (size_t)args->lengths[0],
                           args->args[1], (size_t)args->lengths[1],
                           args->args[2], (size_t)args->lengths[2],
                           container->expiration, (uint16_t)0);
  if (rc != MEMCACHED_SUCCESS)
  {
    log_error(args, "memcached_add_by_key failed, rc=%d\n", rc);
    return (long long)0;  
  }

  return (long long) 1;
}

void memc_add_by_key_deinit(UDF_INIT *initid)
{
  /* if we allocated initid->ptr, free it here */
  memc_function_st *container= (memc_function_st *)initid->ptr;

  memc_free_servers(container->memc);
  free(container);
}
