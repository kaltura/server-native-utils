// headers
#include <sys/inotify.h>
#include <unistd.h>
#include <stdlib.h>
#include <getopt.h>
#include <limits.h>
#include <errno.h>
#include <stdio.h>
#include <zlib.h>

// constants
#define MEMORY_LIMIT (256 * 1024 * 1024)
#define CHUNK_SIZE_COMP (65536)
#define CHUNK_SIZE_READ (65536)
#define INOTIFY_BUF_LEN (10 * (sizeof(struct inotify_event) + NAME_MAX + 1))
#define DEFAULT_N_LINES (10)

// list macros
#define initialize_list_head(list_head) (\
	(list_head)->next = (list_head)->prev = (list_head))

#define insert_head_list(list_head, entry) {	\
	list_entry_t *__list_head, *__next; 		\
	__list_head = (list_head); 					\
	__next = __list_head->next; 				\
	(entry)->next = __next; 					\
	(entry)->prev = __list_head; 				\
	__next->prev = (entry); 					\
	__list_head->next = (entry); 				\
	}

#define remove_entry_list(entry) {				\
	list_entry_t* __old_prev;					\
	list_entry_t* __old_next;					\
	__old_next = (entry)->next;					\
	__old_prev = (entry)->prev;					\
	__old_prev->next = __old_next;				\
	__old_next->prev = __old_prev;				\
	}
	
// getopt
#define case_GETOPT_HELP_CHAR      \
	case GETOPT_HELP_CHAR:         \
	usage (EXIT_SUCCESS);          \
	break;
 
#define GETOPT_HELP_CHAR 'h'

// typedefs
typedef struct list_entry_s {
	struct list_entry_s *next;
	struct list_entry_s *prev;
} list_entry_t;

typedef struct {
	list_entry_t node;
	u_char* data;
	long offset;
	long size;
} buffer_t;

// globals
static list_entry_t buffer_queue;
static int forever = 0;

static long 
get_line_count_from_offset(buffer_t* cur_buffer, long cur_buffer_offset)
{
	z_stream strm;
	u_char out[CHUNK_SIZE_COMP];
	long line_count = 0;
	u_char* end_pos;
	u_char* cur_pos;
	int rc;

	strm.avail_in = 0;
	strm.next_in = Z_NULL;	
	
	for (;;)
	{
		// initialize inflate
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;
		rc = inflateInit2(&strm, 31);
		if (rc != Z_OK)
		{
			return -1;
		}

		do 
		{
			// get an input buffer
			while (strm.avail_in == 0)
			{
				if (cur_buffer_offset >= cur_buffer->size)
				{
					if (cur_buffer->node.next == &buffer_queue)
					{
						return line_count;
					}
					cur_buffer = (buffer_t*)cur_buffer->node.next;
					cur_buffer_offset = 0;
				}
				strm.next_in = cur_buffer->data + cur_buffer_offset;
				strm.avail_in = cur_buffer->size - cur_buffer_offset;
				cur_buffer_offset = cur_buffer->size;
			}

			do 
			{
				// inflate as much as possible
				strm.avail_out = CHUNK_SIZE_COMP;
				strm.next_out = out;

				rc = inflate(&strm, Z_NO_FLUSH);
				switch (rc) 
				{
				case Z_NEED_DICT:
				case Z_DATA_ERROR:
				case Z_MEM_ERROR:
					(void)inflateEnd(&strm);
					return -1;
				}

				// count the number of lines
				end_pos = out + CHUNK_SIZE_COMP - strm.avail_out;
				for (cur_pos = out; cur_pos < end_pos; cur_pos++)
				{
					if (*cur_pos == '\n')
					{
						line_count++;
					}
				}

			} while (strm.avail_out == 0);

			// done when inflate says so
		} while (rc != Z_STREAM_END);

		(void)inflateEnd(&strm);
	}
}

static long
get_line_count(buffer_t* cur_buffer, long* buffer_start_offset)
{
	long line_count;
	u_char* end_pos;
	u_char* cur_pos;

	end_pos = cur_buffer->data + cur_buffer->size;
	for (cur_pos = cur_buffer->data; cur_pos + 1 < end_pos; cur_pos++)
	{
		// check for gzip header
		// Note: we can miss a gzip header in buffer boundary
		if (cur_pos[0] != 0x1f || cur_pos[1] != 0x8b)
		{
			continue;
		}
		
		// get the line count from the current offset
		line_count = get_line_count_from_offset(cur_buffer, cur_pos - cur_buffer->data);
		if (line_count > 0)
		{
			*buffer_start_offset = cur_pos - cur_buffer->data;
			return line_count;
		}
	}
	return -1;
}

int 
print_lines(FILE* source, int inotify_fd, buffer_t* cur_buffer, long cur_buffer_offset, long skip_count)
{
	z_stream strm;
	u_char inotify_buffer[INOTIFY_BUF_LEN];
	u_char out[CHUNK_SIZE_COMP];
	u_char in[CHUNK_SIZE_READ];
	u_char* end_pos;
	u_char* cur_pos;
	int read_file = 0;
	ssize_t bytes_read;
	buffer_t* old_buffer;
	int rc;

	strm.avail_in = 0;
	strm.next_in = Z_NULL;	
	
	for (;;)
	{
		// initialize inflate
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;
		rc = inflateInit2(&strm, 31);
		if (rc != Z_OK)
		{
			printf("inflateInit2 failed %d", rc);
			return 1;
		}

		do 
		{
			// get an input buffer
			while (strm.avail_in == 0)
			{
				if (read_file)
				{
					// already processed all buffers in memory, read from file
					strm.avail_in = fread(in, 1, CHUNK_SIZE_READ, source);
					if (strm.avail_in < 0)
					{
						printf("fread failed %d", errno);
						return 1;
					}
					
					if (strm.avail_in == 0)
					{
						// nothing to read, wait until new data is available
						bytes_read = read(inotify_fd, inotify_buffer, INOTIFY_BUF_LEN);
						if (bytes_read <= 0)
						{
							printf("read inotify failed %d", errno);
							return 1;
						}
						
					}
					strm.next_in = in;
					continue;
				}
			
				if (cur_buffer_offset >= cur_buffer->size)
				{
					// finished handling current buffer, move to the next one
					if (cur_buffer->node.next == &buffer_queue)
					{
						if (!forever)
						{
							return 0;
						}
						read_file = 1;
						continue;
					}

					old_buffer = cur_buffer;					
					cur_buffer = (buffer_t*)cur_buffer->node.next;
					cur_buffer_offset = 0;
					
					remove_entry_list(&old_buffer->node);
					free(old_buffer->data);
					free(old_buffer);
				}
				strm.next_in = cur_buffer->data + cur_buffer_offset;
				strm.avail_in = cur_buffer->size - cur_buffer_offset;
				cur_buffer_offset = cur_buffer->size;
			}

			do 
			{
				// inflate as much as possible
				strm.avail_out = CHUNK_SIZE_COMP;
				strm.next_out = out;

				rc = inflate(&strm, Z_NO_FLUSH);
				switch (rc) 
				{
				case Z_NEED_DICT:
				case Z_DATA_ERROR:
				case Z_MEM_ERROR:
					(void)inflateEnd(&strm);
					return 1;
				}

				// skip lines
				end_pos = out + CHUNK_SIZE_COMP - strm.avail_out;
				for (cur_pos = out; skip_count && cur_pos < end_pos; cur_pos++)
				{
					if (*cur_pos == '\n')
					{
						skip_count--;
					}
				}
				
				// write the remainder
				fwrite(cur_pos, end_pos - cur_pos, 1, stdout);

			} while (strm.avail_out == 0);

			// done when inflate says so
		} while (rc != Z_STREAM_END);

		(void)inflateEnd(&strm);
	}
}

enum
{
	LONG_FOLLOW_OPTION = CHAR_MAX + 1,
};

static struct option const long_options[] =
{
	{"follow", no_argument, NULL, LONG_FOLLOW_OPTION},
	{"lines", required_argument, NULL, 'n'},
	{"help", no_argument, NULL, GETOPT_HELP_CHAR},
	{NULL, 0, NULL, 0}
};

static void
usage (int status)
{
	puts("\
Usage: ztail [OPTION]... [FILE]\n\
");

	printf("\
Print the last %d lines of a gzip file to standard output.\n\
", DEFAULT_N_LINES);

	fputs("\
  -f, --follow             output appended data as the file grows\n\
", stdout);

	printf("\
  -n, --lines=K            output the last K lines, instead of the last %d\n\
", DEFAULT_N_LINES);

	fputs("\
      --help               display this help and exit\n", stdout);

	exit(status);
}
			  
static void
parse_options(int argc, char **argv, long *n_units)
{
	int c;

	while ((c = getopt_long(argc, argv, "n:f", long_options, NULL)) != -1)
	{
		switch (c)
		{
		case 'n':
			if (*optarg == '-')
				++optarg;
			*n_units = atoi(optarg);
			break;

		case 'f':
		case LONG_FOLLOW_OPTION:
			forever = 1;
			break;

		case_GETOPT_HELP_CHAR;

		default:
			usage(EXIT_FAILURE);
		}
	}
}

int main(int argc, char **argv)
{
	buffer_t* new_buffer;
	FILE *source;
	long buffer_start_offset;
	long cur_line_count;
	long initial_offset;
	long bytes_to_read;
	long offset;
	long requested_line_count = DEFAULT_N_LINES;
	int inotify_fd = -1;
	
	// parse the command line
	parse_options(argc, argv, &requested_line_count);
	requested_line_count++;
	
	if (optind + 1 != argc)
	{
		usage(EXIT_FAILURE);
	}
	
	// open the file
	source = fopen(argv[optind], "rb");
	if (source == NULL)
	{
		printf("fopen %s failed %d\n", argv[optind], errno);
		goto error;
	}

	if (forever)
	{
		// set an inotify watch
		inotify_fd = inotify_init();
		if (inotify_fd == -1)
		{
			printf("inotify_init failed %d", errno);
			goto error;
		}

		if (inotify_add_watch(inotify_fd, argv[optind], IN_MODIFY) == -1)
		{
			printf("inotify_add_watch failed %d", errno);
			goto error;
		}	
	}
	
	// seek to the end
	if (fseek(source, 0, SEEK_END) == -1)
	{
		printf("fseek failed (1) %d\n", errno);
		goto error;
	}
	
	initial_offset = ftell(source);
	if (initial_offset == -1)
	{
		printf("ftell failed %d\n", errno);
		goto error;
	}
	
	offset = initial_offset;

	initialize_list_head(&buffer_queue);
	
	for (;;)
	{
		if (offset > CHUNK_SIZE_READ)
		{
			bytes_to_read = CHUNK_SIZE_READ;
		}
		else
		{
			bytes_to_read = offset;
		}
		offset -= bytes_to_read;
		
		if (offset + MEMORY_LIMIT < initial_offset)
		{
			printf("memory limit exceeded\n");
			goto error;
		}

		// allocate a buffer
		new_buffer = (buffer_t*)malloc(sizeof(buffer_t));
		if (new_buffer == NULL)
		{
			printf("malloc failed (1)\n");
			goto error;
		}
		
		new_buffer->data = malloc(bytes_to_read);
		if (new_buffer->data == NULL)
		{
			printf("malloc failed (2)\n");
			goto error;
		}
		
		insert_head_list(&buffer_queue, &new_buffer->node);		
		new_buffer->offset = offset;
		new_buffer->size = bytes_to_read;
		
		// read from the file
		if (fseek(source, offset, SEEK_SET) == -1)
		{
			printf("fseek failed (2) %d\n", errno);
			goto error;
		}
		
		if (fread(new_buffer->data, 1, bytes_to_read, source) != bytes_to_read)
		{
			printf("fread failed %d\n", errno);
			goto error;
		}

		cur_line_count = get_line_count(new_buffer, &buffer_start_offset);
		
		if (cur_line_count >= requested_line_count)
		{
			// read enough data, seek to the initial offset and print the result
			if (fseek(source, initial_offset, SEEK_SET) == -1)
			{
				printf("fseek failed (3) %d\n", errno);
				goto error;
			}
		
			return print_lines(source, inotify_fd, new_buffer, buffer_start_offset, cur_line_count - requested_line_count + 1);
		}
		
		if (offset > 0)
		{
			continue;
		}
		
		if (cur_line_count <= 0)
		{
			goto error;
		}
		
		// reached the beginning of the file, seek to the initial offset and print the result
		if (fseek(source, initial_offset, SEEK_SET) == -1)
		{
			printf("fseek failed (4) %d\n", errno);
			goto error;
		}
		
		return print_lines(source, inotify_fd, new_buffer, buffer_start_offset, 0);
	}
	
	return 0;
	
error:

	// not bothering to clean up anything since the process quits
	return 1;
}
