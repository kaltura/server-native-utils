#define _GNU_SOURCE		// for F_SETPIPE_SZ

// includes
#include <sys/inotify.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <pthread.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <fcntl.h>
#include <stdio.h>
#include <errno.h>
#include <zlib.h>
#include <pwd.h>
#include <grp.h>
#include "buffer_pool.h"
#include "itp.h"


// paths
#define PID_FILE_PATH "/var/run/log_compressor.pid"
#define LOG_FILE_PATH "/var/log/log_compressor.log"

// macros
#define ARRAY_ELEMENTS(x) (sizeof(x) / sizeof(x[0]))

// constants
// Note: memory usage is roughly limited to BUFFER_SIZE_READ x ITP_SIZE_READER_TO_COMP + BUFFER_SIZE_COMP x ITP_SIZE_COMP_TO_WRITER
#define BUFFER_SIZE_READ (65536)
#define BUFFER_SIZE_COMP (65536)
#define ITP_SIZE_READER_TO_COMP (256)
#define ITP_SIZE_COMP_TO_WRITER (256)
#define MIN_READ_BUFFER_SIZE (16384)
#define MAX_UNCOMP_SIZE_TILL_SYNC (64 * 1024 * 1024)

#define FLAG_REOPEN_FILE	(0x1)
#define FLAG_SHUTDOWN		(0x2)
#define FLAG_FLUSH_MASK		(FLAG_REOPEN_FILE | FLAG_SHUTDOWN)

#define INOTIFY_BUF_LEN (10 * (sizeof(struct inotify_event) + NAME_MAX + 1))

#define ZLIB_GZIP_ENCODING (16)

#define REOPEN_SIGNAL SIGUSR1
#define SHUTDOWN_SIGNAL SIGQUIT

#define UNIX_DGRAM_PREFIX "udg://"
#define FILE_PREFIX "file://"

// typedefs
typedef void *(*thread_func_t)(void *arg);

typedef struct {
	int input_fd;
	int input_type;
	int inotify_fd;
	buffer_pool_t read_pool;
	buffer_pool_t comp_pool;
	itp_t reader_to_compressor;
	itp_t compressor_to_writer;
	const char* output_filename;
} state_t;

enum {		// input types
	IT_UNIX_DGRAM,
	IT_PIPE,
	IT_FILE,
};

// globals
static int signals[] = {
	REOPEN_SIGNAL,
	SHUTDOWN_SIGNAL,
	0
};

static volatile int reopen_files = 0;		// gets incremented on every REOPEN_SIGNAL signal
static volatile int shutdown_signalled = 0;

static sem_t thread_error_sem;

static FILE* log_file;

// functions
static void 
log_print(char *format, ...)
{
	va_list ap;
	time_t t = time(NULL);
	struct tm *tm = localtime(&t);

	fprintf(log_file, "%04d-%02d-%02d %02d:%02d:%02d ", tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday, tm->tm_hour, tm->tm_min, tm->tm_sec);
	
	va_start(ap, format);
	vfprintf(log_file, format, ap);
	va_end(ap);
	
	fprintf(log_file, "\n");
	
	fflush(log_file);
}

static void* 
file_writer_thread(void* context)
{
	state_t* state = (state_t*)context;
	itp_buffer_t input_buffer;
	ssize_t bytes_written;
	int output_fd = -1;

	for (;;)
	{
		if (!itp_read(&state->compressor_to_writer, &input_buffer, TRUE))
		{
			log_print("file_writer_thread: itp_read failed");
			goto error;
		}

		if (output_fd == -1)
		{
			if (strcmp(state->output_filename, ".gz") == 0)
			{
				output_fd = STDOUT_FILENO;
			}
			else
			{
				output_fd = open(state->output_filename, O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
				if (output_fd == -1)
				{
					log_print("file_writer_thread: open failed %d", errno);
					goto error;
				}
			}
		}
				
		bytes_written = write(output_fd, input_buffer.ptr, input_buffer.size);
		if (bytes_written != (ssize_t) input_buffer.size) 
		{
			log_print("write failed %d", errno);
			// may happen in case of disk full, just retry next time (the file can get corrupted of course)
		}
		
		buffer_pool_free(&state->comp_pool, input_buffer.ptr);
		
		if ((input_buffer.flags & FLAG_FLUSH_MASK) != 0)
		{
			if (output_fd != STDOUT_FILENO)
			{
				close(output_fd);
				output_fd = -1;
			}
			
			if ((input_buffer.flags & FLAG_SHUTDOWN) != 0)
			{
				return NULL;
			}
		}
	}

error:

	sem_post(&thread_error_sem);
	return NULL;
}

static void *
zlib_alloc(void *opaque, u_int items, u_int size)
{
	return malloc(items * size);
}

static void 
zlib_free(void *opaque, void *address)
{
	free(address);
}

static void* 
compressor_thread(void* context)
{
	state_t* state = (state_t*)context;
	z_stream zstream;
	itp_buffer_t input_buffer;
	itp_buffer_t output_buffer;
	u_char* buffer = NULL;
	bool_t zstream_inited = FALSE;
	size_t bytes_since_sync;
	int flush;
	int rc;

	for (;;)
	{
		if (!itp_read(&state->reader_to_compressor, &input_buffer, TRUE))
		{
			log_print("compressor_thread: itp_read failed");
			goto error;
		}

		if (!zstream_inited)
		{
			memset(&zstream, 0, sizeof(z_stream));

			zstream.zalloc = zlib_alloc;
			zstream.zfree = zlib_free;

			rc = deflateInit2(&zstream, Z_DEFAULT_COMPRESSION, Z_DEFLATED, MAX_WBITS | ZLIB_GZIP_ENCODING, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY);
			if (rc != Z_OK)
			{
				log_print("compressor_thread: deflateInit2 failed %d", rc);
				goto error;
			}
			
			bytes_since_sync = 0;
			
			zstream_inited = TRUE;
		}
		
		flush = ((input_buffer.flags & FLAG_FLUSH_MASK) != 0 || bytes_since_sync > MAX_UNCOMP_SIZE_TILL_SYNC) ? Z_FINISH : Z_NO_FLUSH;
		
		zstream.next_in = input_buffer.ptr;
		zstream.avail_in = input_buffer.size;
		
		bytes_since_sync += input_buffer.size;

		do
		{
			if (zstream.avail_out == 0)
			{
				if (buffer != NULL)
				{
					// write the buffer
					output_buffer.ptr = buffer;
					output_buffer.size = BUFFER_SIZE_COMP;
					output_buffer.flags = 0;
					if (!itp_write(&state->compressor_to_writer, &output_buffer, TRUE))
					{
						log_print("compressor_thread: itp_write failed");
						goto error;
					}
				}
			
				// get a new buffer
				buffer = buffer_pool_alloc(&state->comp_pool);
				if (buffer == NULL)
				{
					log_print("compressor_thread: buffer_pool_alloc failed");
					goto error;
				}
				zstream.next_out = buffer;
				zstream.avail_out = BUFFER_SIZE_COMP;
			}

			rc = deflate(&zstream, flush);
			if (rc != Z_OK && rc != Z_STREAM_END)
			{
				log_print("compressor_thread: deflate failed %d", rc);
				goto error;
			}
			
		} while (zstream.avail_out == 0);
	
		if (rc == Z_STREAM_END)
		{
			output_buffer.ptr = buffer;
			output_buffer.size = BUFFER_SIZE_COMP - zstream.avail_out;
			output_buffer.flags = input_buffer.flags;
			if (!itp_write(&state->compressor_to_writer, &output_buffer, TRUE))
			{
				log_print("compressor_thread: itp_write failed");
				goto error;
			}
			
			buffer = NULL;			
			
			rc = deflateEnd(&zstream);
			if (rc != Z_OK) 
			{
				log_print("compressor_thread: deflateEnd failed %d", rc);
				goto error;
			}
			
			zstream_inited = FALSE;
			
			if ((output_buffer.flags & FLAG_SHUTDOWN) != 0)
			{
				return NULL;
			}
		}
		
		buffer_pool_free(&state->read_pool, input_buffer.ptr);		
	}
	
error:
	
	sem_post(&thread_error_sem);
	return NULL;
}

static void* 
reader_thread(void* context)
{
	state_t* state = (state_t*)context;
	ssize_t bytes_read;
	u_char inotify_buffer[INOTIFY_BUF_LEN];
	itp_buffer_t output_buffer;
	u_char* buffer;
	u_char* next_out;
	bool_t wait = state->input_type == IT_FILE ? TRUE : FALSE;
	size_t avail_out;
	int last_reopen_files = 0;

	// allocate the first buffer
	buffer = buffer_pool_alloc(&state->read_pool);
	if (buffer == NULL)
	{
		log_print("reader_thread: buffer_pool_alloc failed");
		goto error;
	}
	
	next_out = buffer;
	avail_out = BUFFER_SIZE_READ;
	
	for (;;)
	{
		if (avail_out <= MIN_READ_BUFFER_SIZE || last_reopen_files != reopen_files || shutdown_signalled)
		{
			// write the buffer
			output_buffer.ptr = buffer;
			output_buffer.size = BUFFER_SIZE_READ - avail_out;
			if (shutdown_signalled)
			{
				output_buffer.flags = FLAG_SHUTDOWN;
			}
			else
			{
				output_buffer.flags = (last_reopen_files != reopen_files) ? FLAG_REOPEN_FILE : 0;
			}
			
			if (itp_write(&state->reader_to_compressor, &output_buffer, wait))
			{
				if (output_buffer.flags == FLAG_REOPEN_FILE)
				{
					last_reopen_files = reopen_files;
				}
				else if (output_buffer.flags == FLAG_SHUTDOWN)
				{
					return NULL;
				}

				// buffer was sent, allocate a new one
				buffer = buffer_pool_alloc(&state->read_pool);
				if (buffer == NULL)
				{
					log_print("reader_thread: buffer_pool_alloc failed");
					goto error;
				}
			}
			else
			{
				// failed to write the buffer, just read over the current buffer, the data is lost
				log_print("reader_thread: queue full, throwing buffer");
			}
			
			next_out = buffer;
			avail_out = BUFFER_SIZE_READ;
		}
		
		bytes_read = read(state->input_fd, next_out, avail_out);		
		if (bytes_read <= 0)
		{
			if (bytes_read < 0 && errno != EWOULDBLOCK)
			{
				log_print("reader_thread: read failed %d", errno);
				goto error;
			}
			
			if (state->input_type == IT_FILE)
			{
				shutdown_signalled = 1;
				sem_post(&thread_error_sem);		// wake up the main thread
				continue;
			}
			
			if (state->inotify_fd == -1)
			{
				log_print("reader_thread: read returned no data while inotify is not initialized");
				goto error;
			}
		
			bytes_read = read(state->inotify_fd, inotify_buffer, INOTIFY_BUF_LEN);
			if (bytes_read <= 0)
			{
				log_print("reader_thread: read inotify failed %d", errno);
				goto error;
			}

			continue;
		}

		next_out += bytes_read;
		avail_out -= bytes_read;
	}

error:
	
	sem_post(&thread_error_sem);
	return NULL;
}

static void *
sig_thread(void *context)
{
	sigset_t set;
	int sig;
	int rc;

	sigemptyset(&set);
	sigaddset(&set, REOPEN_SIGNAL);
	sigaddset(&set, SHUTDOWN_SIGNAL);
	
	for (;;) 
	{
		rc = sigwait(&set, &sig);
		if (rc != 0)
		{
			log_print("sig_thread: sigwait failed %d", rc);
			goto error;
		}

		switch (sig)
		{
		case REOPEN_SIGNAL:
			log_print("sig_thread: reopening files");
			reopen_files++;
			break;
			
		case SHUTDOWN_SIGNAL:
			log_print("sig_thread: shutting down");
			shutdown_signalled = 1;
			goto error;					// wake up the main thread
		}
	}
	
error:
	
	sem_post(&thread_error_sem);
	return NULL;
}

static void
signal_handler(int signo)
{
}

static bool_t
init_signals()
{
	int *sig;
	struct sigaction sa;

	for (sig = signals; *sig != 0; sig++) 
	{
		memset(&sa, 0, sizeof(sa));
		sa.sa_handler = signal_handler;		// dummy handler to prevent the signal from killing the process
		sigemptyset(&sa.sa_mask);
		
		if (sigaction(*sig, &sa, NULL) == -1) 
		{
			log_print("init_signals: sigaction failed %d", errno);
			return FALSE;
		}
	}

	return TRUE;
}

static bool_t
parse_user_spec(const char* spec, uid_t* uid, gid_t* gid)
{
	struct passwd *user;
	struct group* group;
	const char* colon_pos;
	const char* user_name;
	const char* group_name;
	
	colon_pos = strchr(spec, ':');
	if (colon_pos == NULL)
	{
		user_name = spec;
		group_name = NULL;
	}
	else
	{
		user_name = strndup(spec, colon_pos - spec);
		if (user_name == NULL)
		{
			log_print("parse_user_spec: strndup failed");
			return FALSE;
		}
		group_name = colon_pos + 1;
	}
	
	user = getpwnam(user_name);
	if (user == NULL)
	{
		log_print("parse_user_spec: getpwnam failed %d", errno);
		return FALSE;
	}
	
	*uid = user->pw_uid;
	*gid = user->pw_gid;
	
	if (group_name != NULL)
	{
		group = getgrnam(group_name);
		if (group == NULL)
		{
			log_print("parse_user_spec: getgrnam failed %d", errno);
			return FALSE;
		}
		
		*gid = group->gr_gid;
	}
	
	return TRUE;
}

static bool_t
set_file_owner(const char* path, const char* owner)
{
	uid_t owner_uid;
	uid_t owner_gid;

	if (!parse_user_spec(owner, &owner_uid, &owner_gid))
	{
		return FALSE;
	}
	
	if (chown(path, owner_uid, owner_gid) == -1)
	{
		log_print("set_file_owner: chown failed %d", errno);
		return FALSE;
	}
	
	return TRUE;
}

static bool_t
init_unix_dgram_socket(state_t* state, const char* path, const char* owner)
{
	struct sockaddr_un addr;

	log_print("init_unix_dgram_socket: binding to %s", path);

	state->input_fd = socket(AF_UNIX, SOCK_DGRAM, 0);
	if (state->input_fd == -1)
	{
		log_print("init_unix_dgram_socket: socket failed %d", errno);
		return FALSE;
	}
	
	unlink(path);
	if (strlen(path) > sizeof(addr.sun_path) - 1)
	{
		log_print("init_unix_dgram_socket: path %s too long", path);
		return FALSE;
	}

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
	if (bind(state->input_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1)
	{
		log_print("init_unix_dgram_socket: bind failed %d", errno);
		return FALSE;
	}

	if (!set_file_owner(path, owner))
	{
		return FALSE;
	}
	
	state->inotify_fd = -1;
	
	return TRUE;
}
		
static bool_t
init_pipe(state_t* state, const char* path, const char* owner)
{
#ifdef F_SETPIPE_SZ
	long pipe_size = 1024 * 1024;
#endif // F_SETPIPE_SZ

	log_print("init_pipe: opening %s", path);

	if (mkfifo(path, 0666) == -1)
	{
		if (errno != EEXIST)
		{
			log_print("init_pipe: mkfifo failed %d", errno);
			return FALSE;
		}
	}
	else
	{
		if (!set_file_owner(path, owner))
		{
			return FALSE;
		}
	}

	state->input_fd = open(path, O_RDONLY | O_NONBLOCK);
	if (state->input_fd == -1)
	{
		log_print("init_pipe: open input file failed %d", errno);
		return FALSE;
	}
	
#ifdef F_SETPIPE_SZ
	if (fcntl(state->input_fd, F_SETPIPE_SZ, pipe_size) == -1)
	{
		log_print("init_pipe: failed to set pipe capacity %d", errno);
	}
	else
	{
		log_print("init_pipe: pipe size set to %ld", pipe_size);
	}
#endif // F_SETPIPE_SZ

	state->inotify_fd = inotify_init();
	if (state->inotify_fd == -1)
	{
		log_print("init_pipe: inotify_init failed %d", errno);
		return FALSE;
	}

	if (inotify_add_watch(state->inotify_fd, path, IN_MODIFY) == -1)
	{
		log_print("init_pipe: inotify_add_watch failed %d", errno);
		return FALSE;
	}
	
	return TRUE;
}

static bool_t
init_file(state_t* state, const char* path)
{
	if (path[0] == 0)
	{
		state->input_fd = STDIN_FILENO;
	}
	else
	{
		state->input_fd = open(path, O_RDONLY);
		if (state->input_fd == -1)
		{
			log_print("init_file: open input file failed %d", errno);
			return FALSE;
		}
	}
	
	state->inotify_fd = -1;
		
	return TRUE;
}

static bool_t
init_state(state_t* state, const char* input_owner, char *args)
{
	char input_path[PATH_MAX];
	char* colon_pos;
	int input_type;
	
	if (strncmp(UNIX_DGRAM_PREFIX, args, sizeof(UNIX_DGRAM_PREFIX) - 1) == 0)
	{
		args += sizeof(UNIX_DGRAM_PREFIX) - 1;
		input_type = IT_UNIX_DGRAM;
	}
	else if (strncmp(FILE_PREFIX, args, sizeof(FILE_PREFIX) - 1) == 0)
	{
		args += sizeof(FILE_PREFIX) - 1;
		input_type = IT_FILE;
	}
	else
	{
		input_type = IT_PIPE;
	}
	
	colon_pos = strchr(args, ':');
	if (colon_pos == NULL || (size_t) (colon_pos - args) >=   sizeof(input_path)) 
	{
		log_print("init_state: failed to parse input param %s", args);
		return FALSE;
	}
	
	memcpy(input_path, args, colon_pos - args);
	input_path[colon_pos - args] = '\0';

	switch (input_type)
	{
	case IT_UNIX_DGRAM:
		if (!init_unix_dgram_socket(state, input_path, input_owner))
		{
			return FALSE;
		}
		break;
		
	case IT_PIPE:
		if (!init_pipe(state, input_path, input_owner))
		{
			return FALSE;
		}
		break;

	case IT_FILE:
		if (!init_file(state, input_path))
		{
			return FALSE;
		}
		break;
	}
	
	if (!buffer_pool_init(&state->read_pool, BUFFER_SIZE_READ))
	{
		log_print("init_state: buffer_pool_init failed (1)");
		return FALSE;
	}

	if (!buffer_pool_init(&state->comp_pool, BUFFER_SIZE_COMP))
	{
		log_print("init_state: buffer_pool_init failed (2)");
		return FALSE;
	}
	
	if (!itp_init(&state->reader_to_compressor, ITP_SIZE_READER_TO_COMP))
	{
		log_print("init_state: itp_init failed (1)");
		return FALSE;
	}

	if (!itp_init(&state->compressor_to_writer, ITP_SIZE_COMP_TO_WRITER))
	{
		log_print("init_state: itp_init failed (2)");
		return FALSE;
	}
	
	state->output_filename = colon_pos + 1;
	state->input_type = input_type;
	
	return TRUE;
}

static bool_t
create_pid_file(const char *pid_file)
{
	struct flock fl;
	int fd;
	char buf[100];
	size_t buf_len;
	ssize_t bytes_written;

	fd = open(pid_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	if (fd == -1)
	{
		log_print("create_pid_file: open failed %d", errno);
		return FALSE;
	}

	fl.l_type = F_WRLCK;
	fl.l_whence = SEEK_SET;
	fl.l_start = 0;
	fl.l_len = 0;
	if (fcntl(fd, F_SETLK, &fl) == -1) 
	{
		if (errno  == EAGAIN || errno == EACCES)
		{
			log_print("create_pid_file: pid file %s is locked", pid_file);
			return FALSE;
		}	
		else
		{
			log_print("create_pid_file: fcntl failed %d", errno);
			return FALSE;
		}	
	}

	if (ftruncate(fd, 0) == -1)
	{
		log_print("create_pid_file: ftruncate failed %d", errno);
		return FALSE;
	}	

	sprintf(buf, "%ld\n", (long) getpid());
	buf_len = strlen(buf);
	bytes_written = write(fd, buf, buf_len);
	if (bytes_written != (ssize_t) buf_len)
	{
		log_print("create_pid_file: write failed %d", errno);
		return FALSE;
	}
	
	// leave the file open and locked

	return TRUE;
}

static thread_func_t threads[] = {
	file_writer_thread,
	reader_thread,
	compressor_thread,
};

static bool_t
file_mode_main(const char* path)
{
	pthread_t* cur_tinfo;
	pthread_t tinfos[ARRAY_ELEMENTS(threads)];
	state_t state;
	size_t path_len = strlen(path);
	unsigned thread_index;
	char* args;
	int rc;
	
	log_file = stderr;

	// create a semaphore that threads can use to notify errors
	rc = sem_init(&thread_error_sem, 0, 0);
	if (rc != 0)
	{
		log_print("main_thread: sem_init failed %d", errno);
		return FALSE;
	}
	
	args = malloc(sizeof(FILE_PREFIX) + 2 * path_len + sizeof(":.gz"));
	if (args == NULL)
	{
		log_print("main_thread: malloc failed");
		return FALSE;
	}
	
	sprintf(args, "%s%s:%s.gz", FILE_PREFIX, path, path);

	if (!init_state(&state, NULL, args))
	{
		log_print("main_thread: init_state failed");
		return FALSE;
	}

	cur_tinfo = tinfos;
	for (thread_index = 0; thread_index < ARRAY_ELEMENTS(threads); thread_index++)
	{
		rc = pthread_create(cur_tinfo, NULL, threads[thread_index], &state);
		if (rc != 0)
		{
			log_print("main_thread: pthread_create failed %d", rc);
			return FALSE;
		}
		cur_tinfo++;
	}
	
	rc = sem_wait(&thread_error_sem);
	if (rc != 0)
	{
		log_print("main_thread: sem_wait failed %d", errno);
		return FALSE;
	}
	
	if (shutdown_signalled)
	{
		for (thread_index = 0; thread_index < ARRAY_ELEMENTS(threads); thread_index++)
		{
			pthread_join(tinfos[thread_index], NULL);
		}
	}
	else
	{
		log_print("main_thread: thread error event signalled, quitting...");
	}
	
	return TRUE;
}

static bool_t
main_thread(int argc, char *argv[])
{
	pthread_t sig_thread_info;
	pthread_t* tinfos;
	pthread_t* cur_tinfo;
	sigset_t set;
	state_t* states;
	unsigned thread_index;
	int arg_index;
	int watched_inputs;
	unsigned thread_count;
	int rc;
	
	// create a semaphore that threads can use to notify errors
	rc = sem_init(&thread_error_sem, 0, 0);
	if (rc != 0)
	{
		log_print("main_thread: sem_init failed %d", errno);
		return FALSE;
	}	
	
	// initialize signal processing
	sigemptyset(&set);
	sigaddset(&set, REOPEN_SIGNAL);
	sigaddset(&set, SHUTDOWN_SIGNAL);

	if (sigprocmask(SIG_BLOCK, &set, NULL) == -1)
	{
		log_print("main_thread: sigprocmask failed %d", errno);
		return FALSE;
	}

	if (!init_signals())
	{
		log_print("main_thread: init_signals failed");
		return FALSE;
	}
	
	rc = pthread_create(&sig_thread_info, NULL, sig_thread, NULL);
	if (rc != 0)
	{
		log_print("main_thread: pthread_create failed %d", rc);
		return FALSE;
	}
	
	// init states	
	watched_inputs = argc - 2;
	states = malloc(sizeof(states[0]) * watched_inputs);
	if (states == NULL)
	{
		log_print("main_thread: malloc failed (1)");
		return FALSE;
	}
	
	for (arg_index = 0; arg_index < watched_inputs; arg_index++)
	{
		if (!init_state(&states[arg_index], argv[1], argv[arg_index + 2]))
		{
			log_print("main_thread: init_state failed");
			return FALSE;
		}
	}

	// create threads
	thread_count = watched_inputs * ARRAY_ELEMENTS(threads);
	tinfos = malloc(sizeof(tinfos[0]) * thread_count);
	if (tinfos == NULL)
	{
		log_print("main_thread: malloc failed (2)");
		return FALSE;
	}
	
	cur_tinfo = tinfos;
	for (arg_index = 0; arg_index < watched_inputs; arg_index++)
	{
		for (thread_index = 0; thread_index < ARRAY_ELEMENTS(threads); thread_index++)
		{
			rc = pthread_create(cur_tinfo, NULL, threads[thread_index], &states[arg_index]);
			if (rc != 0)
			{
				log_print("main_thread: pthread_create failed %d", rc);
				return FALSE;
			}
			cur_tinfo++;
		}
	}
	
	log_print("main_thread: started");
	
	rc = sem_wait(&thread_error_sem);
	if (rc != 0)
	{
		log_print("main_thread: sem_wait failed %d", errno);
		return FALSE;
	}
	
	if (shutdown_signalled)
	{
		pthread_join(sig_thread_info, NULL);
		for (thread_index = 0; thread_index < thread_count; thread_index++)
		{
			pthread_join(tinfos[thread_index], NULL);
		}
		
		log_print("main_thread: all threads finished");
	}
	else
	{
		log_print("main_thread: thread error event signalled, quitting...");
	}
	
	return TRUE;
}

int 
main(int argc, char *argv[])
{
	pid_t pid, sid;

	if (argc == 1 && !isatty(STDOUT_FILENO))
	{
		return file_mode_main("") ? 0 : 1;
	}

	// validate args
	if (argc < 3)
	{
		printf("Usage:\n\
  daemon mode:\n\
    log_compressor <owner> <input file>:<output file> [ <input file>:<output file> [ ... ] ]\n\
  file mode:\n\
    log_compressor -f <input file>\n");
		return 1;
	}
    
	// check for file mode
	if (argc == 3 && strcmp(argv[1], "-f") == 0)
	{
		return file_mode_main(argv[2]) ? 0 : 1;
	}
	
	// fork off the parent process
	pid = fork();
	if (pid == -1) 
	{
		printf("main: fork failed %d\n", errno);
		return 1;
	}
	
	if (pid != 0) 
	{
		// parent is done
		return 0;
	}

	// open the log file
	log_file = fopen(LOG_FILE_PATH, "a");
	if (log_file == NULL)
	{
		printf("main: fopen log failed %d\n", errno);
		return 1;
	}

	// create a new sid for the child process
	sid = setsid();
	if (sid < 0) 
	{
		log_print("main: setsid failed %d", errno);
		return 1;
	}

	// close the standard file descriptors
	close(STDIN_FILENO);
	close(STDOUT_FILENO);
	close(STDERR_FILENO);

	// create the pid file
	if (!create_pid_file(PID_FILE_PATH))
	{
		return 1;
	}
	
	// start the main thread
	if (!main_thread(argc, argv))
	{
		return 1;
	}
	
	return 0;
}
