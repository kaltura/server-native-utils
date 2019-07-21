#ifdef STANDARD
/* STANDARD is defined, don't use any mysql functions */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#ifdef __WIN__
typedef unsigned __int64 ulonglong;    /* Microsoft's 64 bit types */
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

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "buffer_pool.h"
#include "itp.h"


#define MAX_IPV4_ADDRESS_LEN sizeof("000.000.000.000") - 1

#define BUFFER_SIZE (16384)
#define ITP_SIZE (256)


#define write_be16(p, w)                \
    {                                   \
    *(p)++ = ((w) >> 8) & 0xFF;         \
    *(p)++ = (w)& 0xFF;                 \
    }

#define write_be32(p, dw)               \
    {                                   \
    *(p)++ = ((dw) >> 24) & 0xFF;       \
    *(p)++ = ((dw) >> 16) & 0xFF;       \
    *(p)++ = ((dw) >> 8) & 0xFF;        \
    *(p)++ = (dw)& 0xFF;                \
    }

#define write_be64(p, qw)               \
    {                                   \
    write_be32(p, (qw) >> 32);          \
    write_be32(p, (qw));                \
    }


/* spinlock implementation from nginx */
typedef intptr_t        ngx_int_t;
typedef uintptr_t       ngx_uint_t;

typedef long                        ngx_atomic_int_t;
typedef unsigned long               ngx_atomic_uint_t;

typedef volatile ngx_atomic_uint_t  ngx_atomic_t;


typedef struct {
    u_char              *data;
    size_t               len;
} str_t;

typedef struct {
    struct sockaddr_in   addr;
    buffer_pool_t        pool;
    itp_t                itp;
    pthread_t            tinfo;
    u_char              *start;
    u_char              *end;
    u_char              *last;
    ngx_atomic_t         write_lock;
} state_t;

static state_t  state;
static int      inited = 0;

static void 
log_error(const char *message, ...)
{
    int         buf_len;
    char        buf[1024];
    time_t      t;
    va_list     ap;
    struct tm  *ts;

    // add a timestamp
    t = time(NULL);
    ts = localtime(&t);
    if (ts != NULL) {
        buf_len = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S ", ts);

    } else {
        buf_len = 0;
    }

    // add the message
    va_start(ap, message);

    vsnprintf(buf + buf_len, sizeof(buf) - 1 - buf_len, message, ap);
    buf[sizeof(buf) - 1] = '\0';

    va_end(ap);

    fprintf(stderr, "%s", buf);
}

/* spinlock implementation from nginx */
#define ngx_atomic_cmp_set(lock, old, set)                                    \
    __sync_bool_compare_and_swap(lock, old, set)

#define ngx_atomic_fetch_add(value, add)                                      \
    __sync_fetch_and_add(value, add)

#define ngx_sched_yield()  sched_yield()

#define ngx_cpu_pause()             __asm__ ("pause")

#define ngx_unlock(lock)    *(lock) = 0

ngx_int_t    ngx_ncpu;

static void
ngx_spinlock(ngx_atomic_t *lock, ngx_atomic_int_t value, ngx_uint_t spin)
{
    ngx_uint_t  i, n;

    for ( ;; ) {

        if (*lock == 0 && ngx_atomic_cmp_set(lock, 0, value)) {
            return;
        }

        if (ngx_ncpu > 1) {

            for (n = 1; n < spin; n <<= 1) {

                for (i = 0; i < n; i++) {
                    ngx_cpu_pause();
                }

                if (*lock == 0 && ngx_atomic_cmp_set(lock, 0, value)) {
                    return;
                }
            }
        }

        ngx_sched_yield();
    }
}


static size_t 
set_command_get_size(str_t *key, str_t *value)
{
    return 32 + key->len + value->len;
}

static u_char *
set_command_write(u_char *p, str_t *key, str_t *value, uint32_t expiration)
{
    size_t  total_body = 8 + key->len + value->len;
    
    *p++ = 0x80;                        // magic
    *p++ = 0x11;                        // opcode (set quiet)
    write_be16(p, key->len);            // key length
    *p++ = 0x08;                        // extras length
    *p++ = 0x00;                        // data type
    write_be16(p, 0);                   // vbucket id
    write_be32(p, total_body);          // total body
    write_be32(p, 0);                   // opaque
    write_be64(p, 0LL);                 // cas
    
    write_be32(p, 0);                   // flags
    write_be32(p, expiration);          // exp
    
    memcpy(p, key->data, key->len);     // key
    p += key->len;    
    memcpy(p, value->data, value->len); // value
    p += value->len;
    return p;
}

static void *
sender_thread(void *context)
{
    int            output_fd = -1;
    state_t       *state = (state_t*)context;
    ssize_t        bytes_written;
    itp_buffer_t   input_buffer;

    for (;;) {
        
        if (!itp_read(&state->itp, &input_buffer, TRUE)) {
            log_error("sender_thread: itp_read failed");
            break;
        }

        if (output_fd == -1) {
            
            if((output_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
                log_error("sender_thread: socket failed %d", errno);
                goto next;
            } 
    
            if (connect(output_fd, (struct sockaddr *)&state->addr, 
                sizeof(state->addr)) < 0)
            {
                log_error("sender_thread: connect failed %d", errno);
                close(output_fd);
                output_fd = -1;
                goto next;
            }
        }
        
        bytes_written = write(output_fd, input_buffer.ptr, input_buffer.size);
        if (bytes_written != input_buffer.size) {
            log_error("sender_thread: write failed %d", errno);
            close(output_fd);
            output_fd = -1;
        }

next:
        
        buffer_pool_free(&state->pool, input_buffer.ptr);
    }
    
    return NULL;
}


/*
 * int 
    memc_setup(
        string ip, 
        int port)
 */
 
my_bool 
memc_async_setup_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    if (args->arg_count != 2 || 
        args->arg_type[0] != STRING_RESULT || args->args[0] == NULL ||  // ip
        args->arg_type[1] != INT_RESULT)                                // port
    {
        strncpy(message, "Usage: memc_setup(<ip>, <port>)", MYSQL_ERRMSG_SIZE);
        return 1;
    }
    
    if (args->lengths[0] > MAX_IPV4_ADDRESS_LEN) {
        strncpy(message, "ip address too long", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    if (inited) {
        strncpy(message, "already initialized", MYSQL_ERRMSG_SIZE);
        return 1;
    }

    return 0;
}

longlong 
memc_async_setup(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
    int       rc;
    char      ip_addr[MAX_IPV4_ADDRESS_LEN + 1];
    longlong  port;
    
    /* parse the address */
    memcpy(ip_addr, args->args[0], args->lengths[0]);
    ip_addr[args->lengths[0]] = '\0';
    
    port = *((longlong*)args->args[1]);
    if (port <= 0 || port >= 65536) {
        log_error("memc_async_setup: invalid port");
        return 0LL;
    }

    if (inet_pton(AF_INET, ip_addr, &state.addr.sin_addr) <= 0) {
        log_error("memc_async_setup: failed to parse ip address");
        return 0LL;
    }
    
    state.addr.sin_family = AF_INET;
    state.addr.sin_port = htons(port); 
    
    /* initialize */
    if (!buffer_pool_init(&state.pool, BUFFER_SIZE)) {
        log_error("memc_async_setup: buffer_pool_init failed");
        return 0LL;
    }

    if (!itp_init(&state.itp, ITP_SIZE)) {
        log_error("memc_async_setup: itp_init failed");
        return 0LL;
    }

    rc = pthread_create(&state.tinfo, NULL, sender_thread, &state);
    if (rc != 0) {
        log_error("memc_async_setup: pthread_create failed %d", rc);
        return 0LL;
    }
    
    ngx_ncpu = sysconf(_SC_NPROCESSORS_ONLN);

    state.start = state.end = state.last = NULL;
    
    state.write_lock = 0;
    inited = 1;
    
    return 1LL;
}

void 
memc_async_setup_deinit(UDF_INIT *initid)
{
}


/*
 * int 
    memc_set(
        string key, 
        string value,
        int expiration)
 */
my_bool 
memc_async_set_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
    if (args->arg_count != 3 || 
        args->arg_type[0] != STRING_RESULT || args->args[0] == NULL || // key
        args->arg_type[1] != STRING_RESULT || args->args[1] == NULL || // value
        args->arg_type[2] != INT_RESULT)                               // exp
    {
        strncpy(message, "Usage: memc_async_set(<key>, <value>, <expiration>)", 
            MYSQL_ERRMSG_SIZE);
        return 1;
    }

	if (!inited) {
        strncpy(message, "Not initialized, call memc_async_setup", 
            MYSQL_ERRMSG_SIZE);
        return 1;
	}
	
    return 0;
}

longlong 
memc_async_set(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error)
{
    str_t          key;
    str_t          value;
    size_t         size;
    u_char        *new_buf;
    uint32_t       expiration;
    itp_buffer_t   send_buf;

    /* get the params */
    key.data = args->args[0];
    key.len = (size_t)args->lengths[0];
    value.data = args->args[1];
    value.len = (size_t)args->lengths[1];
    expiration = (uint32_t)*((longlong*)args->args[2]);
    
    if (key.data == NULL) {
        key.len = 0;
    }

    if (value.data == NULL) {
        value.len = 0;
    }

    size = set_command_get_size(&key, &value);
    
    send_buf.size = 0;
    
    ngx_spinlock(&state.write_lock, 1, 2048);
        
    if (state.end - state.last < size) {

        if (size > BUFFER_SIZE) {
            ngx_unlock(&state.write_lock);
            log_error("memc_async_set: key/value too large");
            return 0LL;
        }

        new_buf = buffer_pool_alloc(&state.pool);
        if (new_buf == NULL) {
            ngx_unlock(&state.write_lock);
            log_error("memc_async_set: failed to alloc buffer");
            return 0LL;
        }

        if (state.last > state.start) {
            send_buf.ptr = state.start;
            send_buf.size = state.last - state.start;
            send_buf.flags = 0;
        }
                
        state.start = new_buf;
        state.end = state.start + BUFFER_SIZE;
        state.last = state.start;
    }
    
    state.last = set_command_write(state.last, &key, &value, expiration);
    ngx_unlock(&state.write_lock);
    
    if (send_buf.size > 0) {
        
        if (!itp_write(&state.itp, &send_buf, FALSE)) {
            log_error("memc_async_set: queue full, throwing buffer");
        }
    }

    return 1LL;
}

void 
memc_async_set_deinit(UDF_INIT *initid)
{
}

#endif /* HAVE_DLOPEN */
