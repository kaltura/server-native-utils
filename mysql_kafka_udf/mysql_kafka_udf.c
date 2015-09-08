#ifdef STANDARD
/* STANDARD is defined, don't use any mysql functions */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef __WIN__
typedef unsigned __int64 ulonglong;	/* Microsoft's 64 bit types */
typedef __int64 longlong;
#else
typedef unsigned long long ulonglong;
typedef long long longlong;
#endif /*__WIN__*/
#else
#include <my_global.h>
#include <my_sys.h>

#if defined(MYSQL_SERVER)
#include <m_string.h>
#else
#include <string.h>
#endif

#endif
#include <mysql.h>
#include <ctype.h>

#ifdef _WIN32
/* inet_aton needs winsock library */
#pragma comment(lib, "ws2_32")
#endif

#ifdef HAVE_DLOPEN

#include <librdkafka/rdkafka.h>
#include "hash.h"

// typedefs
typedef struct 
{
	hash_entry_t link;
	rd_kafka_topic_conf_t* conf;
	rd_kafka_topic_t* rkt;
} topic_hash_item_t;

// globals
static rd_kafka_conf_t* conf = NULL;
static rd_kafka_t* rk = NULL;
static hash_table_t topic_hash;
static my_bool topic_hash_initialized = 0;


void log_error(const char* message, ...)
{
	char buf[1024];
	time_t t;
	struct tm* ts;
	va_list ap;
	int buf_len;

	// add a timestamp
	t = time(NULL);
	ts = localtime(&t);
	if (ts != NULL) 
	{
		if (strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S ", ts) == 0) 
		{
			buf[0] = '\0';
		}
	}

	// add the message
	va_start(ap, message);

	buf_len = strlen(buf);
	vsnprintf(buf + buf_len, sizeof(buf) - 1 - buf_len, message, ap);
	buf[sizeof(buf) - 1] = '\0';

	va_end(ap);

	fprintf(stderr, "%s", buf);
}


static my_bool
init_kafka_conf()
{
	if (conf != NULL)
	{
		return 1;
	}
	
	conf = rd_kafka_conf_new();
	if (conf == NULL)
	{
		log_error("rd_kafka_conf_new failed\n");
		return 0;
	}
	
	return 1;
}

static my_bool 
init_kafka()
{
	char message[256];

	if (rk != NULL)
	{
		return 1;
	}
	
	if (!init_kafka_conf())
	{
		return 0;
	}
	
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, message, sizeof(message));
	if (rk == NULL)
	{
		log_error("rd_kafka_new failed, msg=%s\n", message);
		return 0;
	}
	
	return 1;
}

static topic_hash_item_t*
get_topic_hash_item(const char* name, size_t name_length)
{
	topic_hash_item_t* hash_item;
	hash_entry_t* hash_entry;
	
	if (!topic_hash_initialized)
	{
		hash_init(&topic_hash);
		topic_hash_initialized = 1;
	}
	
	// XXXXXX locking
	hash_entry = hash_lookup(&topic_hash, name, name_length);
	if (hash_entry != NULL)
	{
		return container_of(hash_entry, topic_hash_item_t, link);
	}

	// XXXX cleanup in case of error + error messages
	hash_item = calloc(sizeof(*hash_item), 1);
	if (hash_item == NULL)
	{
		log_error("failed to allocate topic hash item\n");
		return NULL;
	}

	hash_item->link.key = malloc(name_length + 1);
	if (hash_item->link.key == NULL)
	{
		log_error("failed to allocate topic name\n");
		return NULL;
	}
	memcpy(hash_item->link.key, name, name_length);
	hash_item->link.key[name_length] = '\0';
	
	hash_item->link.key_length = name_length;

	hash_item->conf = rd_kafka_topic_conf_new();
	if (hash_item->conf == NULL)
	{
		log_error("rd_kafka_topic_conf_new failed\n");
		return NULL;
	}
	
	hash_add(&topic_hash, &hash_item->link);

	return hash_item;
}

/*
 * int 
	kafka_conf_set(
		string key, 
		string value)
 */
my_bool kafka_conf_set_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 2 || 
		args->arg_type[0] != STRING_RESULT || args->args[0] == NULL ||	// key
		args->arg_type[1] != STRING_RESULT || args->args[1] == NULL)	// value
	{
		strncpy(message, "Usage: kafka_conf_set(<key>, <value>)", MYSQL_ERRMSG_SIZE);
		return 1;
	}

	return 0;
}

longlong kafka_conf_set(UDF_INIT* initid, UDF_ARGS* args,
                   char* is_null,
                   char* error)
{
	rd_kafka_conf_res_t rc;
	char message[256];

	if (!init_kafka_conf())
	{
		return 0LL;
	}

	rc = rd_kafka_conf_set(conf, args->args[0], args->args[1], message, sizeof(message));
	if (rc != RD_KAFKA_CONF_OK)
	{
		log_error("rd_kafka_conf_set failed, rc=%d msg=%s\n", rc, message);
		return 0LL;
	}

	return 1LL;
}

void kafka_conf_set_deinit(UDF_INIT* initid)
{
}


/*
 * kafka_brokers_add(
		string brokers)
 */
my_bool kafka_brokers_add_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 1 || 
		args->arg_type[0] != STRING_RESULT || args->args[0] == NULL)	// brokers
	{
		strncpy(message, "Usage: kafka_brokers_add(<brokers>)", MYSQL_ERRMSG_SIZE);
		return 1;
	}

	return 0;
}

longlong kafka_brokers_add(UDF_INIT* initid, UDF_ARGS* args,
                   char* is_null,
                   char* error)
{
	int rc;
	
	if (!init_kafka())
	{
		return 0LL;
	}

	rc = rd_kafka_brokers_add(rk, args->args[0]);
	if (rc == 0) 
	{
		log_error("rd_kafka_brokers_add failed\n");
	}

	return rc;
}

void kafka_brokers_add_deinit(UDF_INIT* initid)
{
}


/*
 * kafka_topic_conf_set(
		string topic,
		string key, 
		string value)
 */
my_bool kafka_topic_conf_set_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 3 || 
		args->arg_type[0] != STRING_RESULT || args->args[0] == NULL ||	// topic
		args->arg_type[1] != STRING_RESULT || args->args[1] == NULL || 	// key
		args->arg_type[2] != STRING_RESULT || args->args[2] == NULL)	// value
	{
		strncpy(message, "Usage: kafka_topic_conf_set(<topic>, <key>, <value>)", MYSQL_ERRMSG_SIZE);
		return 1;
	}

	return 0;
}

longlong kafka_topic_conf_set(UDF_INIT* initid, UDF_ARGS* args,
                   char* is_null,
                   char* error)
{
	rd_kafka_conf_res_t rc;
	topic_hash_item_t* hash_item;
	char message[256];

	hash_item = get_topic_hash_item(args->args[0], args->lengths[0]);
	if (hash_item == NULL)
	{
		return 0LL;
	}
	
	rc = rd_kafka_topic_conf_set(hash_item->conf, args->args[1], args->args[2], message, sizeof(message));
	if (rc != RD_KAFKA_CONF_OK)
	{
		log_error("rd_kafka_topic_conf_set failed, rc=%d msg=%s\n", rc, message);
		return 0LL;
	}

	return 1LL;
}

void kafka_topic_conf_set_deinit(UDF_INIT* initid)
{
}


/*
 * kafka_produce(
		string topic, 
		int partition, 
		string payload, 
		string optional key)
 */
my_bool kafka_produce_init(UDF_INIT* initid, UDF_ARGS* args, char* message)
{
	if (args->arg_count != 4 || 
		args->arg_type[0] != STRING_RESULT || args->args[0] == NULL ||		// topic
		args->arg_type[1] != INT_RESULT || 									// partition
		args->arg_type[2] != STRING_RESULT || args->args[2] == NULL ||		// payload
		args->arg_type[3] != STRING_RESULT)									// key
	{
		strncpy(message, "Usage: kafka_produce_init(<topic>, <partition>, <payload>, <key>)", MYSQL_ERRMSG_SIZE);
		return 1;
	}

	return 0;
}

longlong kafka_produce(UDF_INIT* initid, UDF_ARGS* args,
                   char* is_null,
                   char* error)
{
	topic_hash_item_t* hash_item;
	int rc;

	hash_item = get_topic_hash_item(args->args[0], args->lengths[0]);
	if (hash_item == NULL)
	{
		return 0LL;
	}
	
	if (hash_item->rkt == NULL)
	{
		if (!init_kafka())
		{
			return 0LL;
		}
		
		hash_item->rkt = rd_kafka_topic_new(rk, args->args[0], hash_item->conf);
		if (hash_item->rkt == NULL)
		{
			return 0LL;
		}
	}
	
	rc = rd_kafka_produce(
		hash_item->rkt, 
		*((longlong*)args->args[1]), 
		RD_KAFKA_MSG_F_COPY,
		args->args[2], 
		args->lengths[2],
		args->args[3], 
		args->lengths[3],
		NULL);
	if (rc != RD_KAFKA_CONF_OK)
	{
		log_error("kafka_produce failed, rc=%d errno=%d\n", rc, errno);
		return 0LL;
	}

	return 1LL;
}

void kafka_produce_deinit(UDF_INIT* initid)
{
}

#endif /* HAVE_DLOPEN */
