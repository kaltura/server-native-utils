#include <getopt.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>
#include <stdio.h>
#include <pcre.h>
#include <zlib.h>

// macros
#define min(x, y) (((x) < (y)) ? (x) : (y))

// constants
#define MEMORY_LIMIT (256 * 1024 * 1024)
#define CHUNK_SIZE_COMP (65536)
#define CHUNK_SIZE_READ (65536)
#define MAX_BUFFERS (MEMORY_LIMIT / CHUNK_SIZE_READ)

// enums
typedef enum {
	COMPARE_LESS_THAN,
	COMPARE_GREATER_THAN,
	COMPARE_EQUALS,
	COMPARE_ERROR,
	COMPARE_LIMIT,
} compare_result_t;

enum {
	PM_UNDEFINED,
	PM_NO_FILENAME,
	PM_WITH_FILENAME,
};

enum { EXIT_ERROR = 2 };

// typedefs
typedef struct buffer_s {
	struct buffer_s* next;
	u_char* data;
	size_t size;
} buffer_t;

typedef struct {
	pcre *code;
	pcre_extra *extra;
} regex_t;

typedef struct {
	size_t len;
	u_char* data;
} string_t;

typedef struct {
	FILE *stream;
	string_t prefix;
	int write_prefix;
} prefix_writer_context_t;

typedef void (*write_func_t)(void* context, const u_char* ptr, size_t size);

// globals
static int show_help = 0;
static char* program_name;

const char* file_name;
static regex_t regex;
static string_t start_value = { 0, NULL };
static string_t end_value = { 0, NULL };

static size_t buffers_left = MAX_BUFFERS;
static buffer_t* used_buffers = NULL;
static buffer_t* free_buffers = NULL;

// constants
static char const short_options[] = "e:p:hH";
static struct option const long_options[] =
{
  {"no-filename", no_argument, NULL, 'h'},
  {"with-filename", no_argument, NULL, 'H'},
  {"pattern",    required_argument, NULL, 'p'},
  {"end",    required_argument, NULL, 'e'},
  {"help", no_argument, &show_help, 1},
  {0, 0, 0, 0}
};

static void 
verror(int errnum, const char *message, va_list args)
{
	char const *s;
	
	fflush(stdout);

	fprintf(stderr, "%s: ", program_name);
	vfprintf(stderr, message, args);
	if (errnum)
	{
		s = strerror (errnum);
		fprintf (stderr, ": %s", s);		
	}
	putc('\n', stderr);
	fflush(stderr);
}

void
error(int errnum, const char *message, ...)
{
	va_list args;

	va_start(args, message);
	verror(errnum, message, args);
	va_end(args);
}

static buffer_t*
alloc_read_buffer()
{
	buffer_t* new_buffer;

	if (free_buffers != NULL)
	{
		new_buffer = free_buffers;
		free_buffers = free_buffers->next;
		return new_buffer;
	}
	
	if (buffers_left <= 0)
	{
		error(0, "memory limit exceeded");
		return NULL;
	}

	new_buffer = (buffer_t*)malloc(sizeof(buffer_t));
	if (new_buffer == NULL)
	{
		error(0, "malloc failed (1)");
		return NULL;
	}
	
	new_buffer->data = malloc(CHUNK_SIZE_READ);
	if (new_buffer->data == NULL)
	{
		error(0, "malloc failed (2)");
		return NULL;
	}
	
	buffers_left--;

	return new_buffer;
}

static compare_result_t
compare_first_match(u_char* start_pos, u_char* end_pos, string_t* compare)
{
	size_t compare_len;
	int compare_result;
	int captures[6];
	u_char* cur_pos;
	
	for (cur_pos = start_pos; cur_pos < end_pos; cur_pos++)
	{
		if (*cur_pos != '\n')
		{
			continue;
		}
			
		if (pcre_exec(
			regex.code, 
			regex.extra, 
			(const char *) cur_pos + 1, 
			end_pos - (cur_pos + 1), 
			0, 
			0, 
			captures, 
			sizeof(captures) / sizeof(captures[0])) < 2)
		{
			continue;
		}
		
		compare_len = captures[3] - captures[2];
		if (compare->len < compare_len)
		{
			compare_len = compare->len;
		}

		compare_result = memcmp(cur_pos + 1 + captures[2], compare->data, compare_len);
		if (compare_result < 0)
		{
			return COMPARE_LESS_THAN;
		}
		else if (compare_result > 0)
		{
			return COMPARE_GREATER_THAN;
		}
		else
		{
			return COMPARE_EQUALS;
		}
	}
	
	return COMPARE_ERROR;
}

static compare_result_t
compare_last_match(u_char* start_pos, u_char* end_pos, string_t* compare)
{
	size_t compare_len;
	int compare_result;
	int captures[6];
	u_char* cur_pos;
	
	for (cur_pos = end_pos - 1; cur_pos >= start_pos; cur_pos--)
	{
		if (*cur_pos != '\n')
		{
			continue;
		}
			
		if (pcre_exec(
			regex.code, 
			regex.extra, 
			(const char *) cur_pos + 1, 
			end_pos - (cur_pos + 1), 
			0, 
			0, 
			captures, 
			sizeof(captures) / sizeof(captures[0])) < 2)
		{
			continue;
		}
		
		compare_len = captures[3] - captures[2];
		if (compare->len < compare_len)
		{
			compare_len = compare->len;
		}

		compare_result = memcmp(cur_pos + 1 + captures[2], compare->data, compare_len);
		if (compare_result < 0)
		{
			return COMPARE_LESS_THAN;
		}
		else if (compare_result > 0)
		{
			return COMPARE_GREATER_THAN;
		}
		else
		{
			return COMPARE_EQUALS;
		}
	}

	return COMPARE_ERROR;
}

static compare_result_t 
inflate_compare_first_match(buffer_t* cur_buffer, size_t cur_buffer_offset)
{
	compare_result_t result;
	z_stream strm;
	u_char out[CHUNK_SIZE_COMP];
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
			return COMPARE_ERROR;
		}

		do 
		{
			// get an input buffer
			while (strm.avail_in == 0)
			{
				if (cur_buffer_offset >= cur_buffer->size)
				{
					if (cur_buffer->next == NULL)
					{
						(void)inflateEnd(&strm);
						return COMPARE_ERROR;
					}
					cur_buffer = (buffer_t*)cur_buffer->next;
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
					return COMPARE_ERROR;
				}

				// compare the first match
				// Note: can miss a line in buffer boundary
				result = compare_first_match(out, out + CHUNK_SIZE_COMP - strm.avail_out, &start_value);
				if (result != COMPARE_ERROR)
				{
					(void)inflateEnd(&strm);
					return result;
				}
			} while (strm.avail_out == 0);

			// done when inflate says so
		} while (rc != Z_STREAM_END);

		(void)inflateEnd(&strm);
	}
}

static compare_result_t
search_compare_first_match(buffer_t* cur_buffer, size_t* buffer_start_offset)
{
	compare_result_t result;
	u_char* start_pos = cur_buffer->data;
	u_char* cur_pos;

	// check for gzip header at the last byte
	if (start_pos[cur_buffer->size - 1] == 0x1f &&
		cur_buffer->next != NULL &&
		cur_buffer->next->data[0] == 0x8b)
	{
		result = inflate_compare_first_match(cur_buffer, cur_buffer->size - 1);
		if (result != COMPARE_ERROR)
		{
			*buffer_start_offset = cur_buffer->size - 1;
			return result;
		}
	}
	
	for (cur_pos = start_pos + cur_buffer->size - 2; cur_pos >= start_pos; cur_pos--)
	{
		// check for gzip header
		if (cur_pos[0] != 0x1f || cur_pos[1] != 0x8b)
		{
			continue;
		}
		
		// get the line count from the current offset
		result = inflate_compare_first_match(cur_buffer, cur_pos - start_pos);
		if (result != COMPARE_ERROR)
		{
			*buffer_start_offset = cur_pos - start_pos;
			return result;
		}
	}
	return COMPARE_ERROR;
}

static compare_result_t 
compare_file_offset(FILE *source, off_t offset, off_t limit, off_t* start_offset)
{
	size_t buffer_start_offset;
	compare_result_t result = COMPARE_ERROR;
	buffer_t* last_buffer;
	buffer_t* new_buffer;
	size_t bytes_to_read;
	
	// alloc the initial buffer
	new_buffer = alloc_read_buffer();
	if (new_buffer == NULL)
	{
		return COMPARE_ERROR;
	}
	
	last_buffer = new_buffer;
	
	for (;;)
	{
		// get the number of bytes to read
		if (offset > CHUNK_SIZE_READ)
		{
			bytes_to_read = CHUNK_SIZE_READ;
		}
		else
		{
			bytes_to_read = offset;
		}

		if ((off_t)(offset - bytes_to_read) < limit)
		{
			bytes_to_read = offset - limit;
		}
		
		offset -= bytes_to_read;
		
		// update the buffer
		new_buffer->size = bytes_to_read;
		new_buffer->next = used_buffers;
		used_buffers = new_buffer;
		
		// read from the file
		if (fseek(source, offset, SEEK_SET) == -1)
		{
			error(errno, "%s", file_name);
			break;
		}
		
		if (fread(new_buffer->data, 1, bytes_to_read, source) != bytes_to_read)
		{
			error(errno, "%s", file_name);
			break;
		}
	
		// compare the current list of buffers
		result = search_compare_first_match(new_buffer, &buffer_start_offset);
		if (result != COMPARE_ERROR)
		{
			*start_offset = offset + buffer_start_offset;
			break;
		}
		
		if (offset <= limit)
		{
			result = COMPARE_LIMIT;
			break;
		}
		
		if (offset <= 0)
		{
			error(0, "%s: reached the beginning of the file", file_name);
			break;
		}
		
		// allocate another buffer
		new_buffer = alloc_read_buffer();
		if (new_buffer == NULL)
		{
			break;
		}
	}

	// move the used buffers list to the free buffers list
	last_buffer->next = free_buffers;
	free_buffers = used_buffers;
	used_buffers = NULL;			
	return result;
}

static void
init_prefix_writer(prefix_writer_context_t* context, FILE *stream, string_t* prefix)
{
	context->stream = stream;
	context->prefix = *prefix;
	context->write_prefix = 1;
}

static void 
write_prefix(prefix_writer_context_t* context, const u_char* ptr, size_t size)
{
	const u_char* start = ptr;
	const u_char* end = ptr + size;

	for (; ptr < end; ptr++)
	{
		if (context->write_prefix)
		{
			fwrite(start, ptr - start, 1, context->stream);
			start = ptr;
			
			fwrite(context->prefix.data, context->prefix.len, 1, context->stream);
			fwrite(": ", 2, 1, context->stream);
			context->write_prefix = 0;
		}
		
		if (*ptr != '\n')
		{
			continue;
		}
		
		context->write_prefix = 1;
	}

	fwrite(start, end - start, 1, context->stream);
}

static void
write_simple(FILE* stream, const u_char* ptr, size_t size)
{
	fwrite(ptr, size, 1, stream);
}

static int 
print_lines(FILE* source, string_t* prefix)
{
	prefix_writer_context_t prefix_writer;
	write_func_t write_func;
	void* write_context;
	compare_result_t result;
	z_stream strm;
	u_char out[CHUNK_SIZE_COMP];
	u_char in[CHUNK_SIZE_READ];
	u_char* start_pos;
	u_char* end_pos;
	u_char* cur_pos;
	string_t* compare = &start_value;
	size_t compare_len;
	int captures[6];
	int print = 0;
	int rc;

	strm.avail_in = 0;
	strm.next_in = Z_NULL;	
	
	if (prefix != NULL)
	{
		init_prefix_writer(&prefix_writer, stdout, prefix);
		write_func = (write_func_t)write_prefix;
		write_context = &prefix_writer;
	}
	else
	{
		write_func = (write_func_t)write_simple;
		write_context = stdout;
	}
	
	for (;;)
	{
		// initialize inflate
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;
		rc = inflateInit2(&strm, 31);
		if (rc != Z_OK)
		{
			error(0, "inflateInit2 failed %d", rc);
			return 1;
		}

		do 
		{
			// get an input buffer
			if (strm.avail_in == 0)
			{
				strm.avail_in = fread(in, 1, CHUNK_SIZE_READ, source);
				if (strm.avail_in < 0)
				{
					error(errno, "%s", file_name);
					(void)inflateEnd(&strm);
					return 1;
				}
				
				if (strm.avail_in == 0)
				{
					if (!print)
					{
						error(0, "%s: no matching lines, start too big", file_name);
					}
					(void)inflateEnd(&strm);
					return 0;
				}
				strm.next_in = in;
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

				cur_pos = out;
				end_pos = out + CHUNK_SIZE_COMP - strm.avail_out;

				// if the last line does not change the state, we can process the chunk as a whole
				result = compare_last_match(cur_pos, end_pos, compare);
				if (!print)
				{
					if (result != COMPARE_EQUALS && result != COMPARE_GREATER_THAN)
					{
						continue;
					}
				}
				else
				{
					if (result != COMPARE_GREATER_THAN)
					{
						write_func(write_context, cur_pos, end_pos - cur_pos);
						continue;
					}
				}
				
				// process the chunk line by line
				start_pos = cur_pos;
				
				for (cur_pos = out; cur_pos < end_pos; cur_pos++)
				{
					if (*cur_pos != '\n')
					{
						continue;
					}
						
					// match the current line
					if (pcre_exec(
						regex.code, 
						regex.extra, 
						(const char *) cur_pos + 1, 
						end_pos - (cur_pos + 1), 
						0, 
						0, 
						captures, 
						sizeof(captures) / sizeof(captures[0])) < 2)
					{
						continue;
					}
					
					compare_len = captures[3] - captures[2];
					if (compare->len < compare_len)
					{
						compare_len = compare->len;
					}

					if (!print)
					{
						if (memcmp(cur_pos + 1 + captures[2], compare->data, compare_len) < 0)
						{
							continue;
						}

						// compare the current line to the end pattern
						compare = &end_value;
						compare_len = captures[3] - captures[2];
						if (compare->len < compare_len)
						{
							compare_len = compare->len;
						}

						if (memcmp(cur_pos + 1 + captures[2], compare->data, compare_len) > 0)
						{
							error(0, "%s: no matching lines, end too small", file_name);
							(void)inflateEnd(&strm);
							return 0;
						}
						
						start_pos = cur_pos + 1;
						print = 1;
					}
					else
					{
						if (memcmp(cur_pos + 1 + captures[2], compare->data, compare_len) <= 0)
						{
							continue;
						}

						// passed the end pattern -> done
						write_func(write_context, start_pos, cur_pos - start_pos);
						write_func(write_context, (u_char*)"\n", 1);
						(void)inflateEnd(&strm);
						return 0;
					}
				}
				
				if (print)
				{
					write_func(write_context, start_pos, end_pos - start_pos);
				}

			} while (strm.avail_out == 0);

			// done when inflate says so
		} while (rc != Z_STREAM_END);

		(void)inflateEnd(&strm);
	}
}

static int 
process_file(int file_name_prefix)
{
	compare_result_t result;
	string_t prefix;
	FILE *source;
	off_t buffer_start_offset;
	off_t limit = -1;
	off_t left = 0;
	off_t right;
	off_t mid;
	int status = 1;

	// open the file
	source = fopen(file_name, "rb");
	if (source == NULL)
	{
		error(errno, "%s", file_name);
		return 1;
	}

	// seek to the end
	if (fseek(source, 0, SEEK_END) == -1)
	{
		error(errno, "%s", file_name);
		goto error;
	}
	
	right = ftell(source);
	if (right == -1)
	{
		error(errno, "%s", file_name);
		goto error;
	}

	// binary search for the start pattern
	mid = (left + right) / 2;
	while (left <= right)
	{
		result = compare_file_offset(source, mid, limit, &buffer_start_offset);
	#if 0
		printf("left=%ld right=%ld mid=%ld result=%d start=%ld\n", left, right, mid, result, buffer_start_offset);
	#endif
		switch (result)
		{
		case COMPARE_ERROR:
			goto error;

		case COMPARE_LIMIT:
			left = mid + 1;
			// reaching the limit is an indication that left are right are close, set mid to right - 1
			//	so that if left and right are on the same zip chunk the search will complete immediately
			if (left < right)
			{
				mid = right - 1;
			}
			else
			{
				mid = left;
			}
			continue;

		case COMPARE_LESS_THAN:
			left = mid + 1;
			limit = buffer_start_offset + 1;
			break;
			
		case COMPARE_EQUALS:
		case COMPARE_GREATER_THAN:
			right = buffer_start_offset - 1;
			break;
		}

		mid = (left + right) / 2;
	}
	
	// seek to the start of the zip chunk
	if (left > 1)
	{
		compare_file_offset(source, left - 1, -1, &buffer_start_offset);
	}
	else
	{
		buffer_start_offset = 0;
	}
	
	if (fseek(source, buffer_start_offset, SEEK_SET) == -1)
	{
		error(errno, "%s", file_name);
		goto error;
	}	
	
	// print the relevant lines
	prefix.data = (u_char*)file_name;
	prefix.len = strlen(file_name);
	
	if (print_lines(source, file_name_prefix ? &prefix : NULL) != 0)
	{
		goto error;
	}

	status = 0;

error:

	fclose(source);
	return status;
}

static void
usage(int status)
{
	if (status != 0)
	{
		fprintf (stderr, "Usage: %s [OPTION]... START [FILE]...\n", program_name);
		fprintf (stderr, "Try '%s --help' for more information.\n", program_name);
	}
	else
    {
      printf ("Usage: %s [OPTION]... START [FILE]...\n", program_name);
      printf ("\
Print lines between START and END in each FILE using binary search.\n\
A regular expression is executed on each line to capture the value that\n\
should be compared to START / END (see -p below).\n\
The input files must be sorted according to the value captured by \n\
the regular expression. And, in addition, they must be gzip files\n\
that are periodically flushed, and therefore enable reading at \n\
multiple offsets.\n");
      printf ("\
Example: %s -p '(\\d{2}:\\d{2}:\\d{2})' '02:45:00' input.log.gz\n\
\n", program_name);
		printf ("\
\n\
      --help                display this help text and exit\n\
  -H, --with-filename       print the file name for each match\n\
  -h, --no-filename         suppress the file name prefix on output\n\
  -e, --end                 END value, defaults to START if not specified\n\
  -p, --pattern             a regular expression that captures the value\n\
                            that should be compared to START / END, \n\
                            for each line. \n\
                            the default pattern is (.*)\n\
");
	}
	exit(status);
}

int main(int argc, char **argv)
{
	char* pattern = "(.*)";
	const char *errstr;
	int prefix_mode = PM_UNDEFINED;
	int erroff;
	int opt;
	int rc;

	// parse the command line
	program_name = argv[0];
	
	for (;;)
	{
		opt = getopt_long(argc, (char **) argv, short_options, long_options, NULL);
		if (opt == -1)
		{
			break;
		}

		switch (opt)
		{
			case 'h':
				prefix_mode = PM_NO_FILENAME;
				break;
				
			case 'H':
				prefix_mode = PM_WITH_FILENAME;
				break;
				
			case 'e':
				end_value.data = (u_char*)optarg;
				end_value.len = strlen((char*)end_value.data);
				break;
				
			case 'p':
				pattern = optarg;
				break;

			case 0:
				// long options
				break;
				
			default:
				usage(EXIT_ERROR);
				break;
		}
	}

	if (show_help)
	{
		usage(EXIT_SUCCESS);
	}

	if (optind + 2 > argc)
    {
		usage(EXIT_SUCCESS);
	}
	
	start_value.data = (u_char*)argv[optind++];
	start_value.len = strlen((char*)start_value.data);

	if (end_value.len == 0)
	{
		end_value = start_value;
	}
	else if (memcmp(end_value.data, start_value.data, min(start_value.len, end_value.len)) < 0)
	{
		error(0, "end pattern smaller than start pattern");
		return 1;
	}
	
	if (prefix_mode == PM_UNDEFINED)
	{
		if (argc - optind > 1)
		{
			prefix_mode = PM_WITH_FILENAME;
		}
		else
		{
			prefix_mode = PM_NO_FILENAME;
		}
	}
	
	// initialize the regex
	regex.code = pcre_compile(pattern, 0, &errstr, &erroff, NULL);
	if (regex.code == NULL) 
	{
		if ((size_t) erroff == strlen(pattern)) 
		{
			error(0, "pcre_compile() failed: %s in \"%s\"", errstr, pattern);
		} 
		else 
		{
			error(0, "pcre_compile() failed: %s in \"%s\" at \"%s\"", errstr, pattern, pattern + erroff);
		}

		return 1;
	}

	regex.extra = pcre_study(regex.code, 0, &errstr);
	if (errstr != NULL) 
	{
		error(0, "pcre_study() failed: %s", errstr);
	}
	
	// process the files
	rc = 0;
	while (optind < argc)
	{
		file_name = argv[optind++];
		if (process_file(prefix_mode == PM_WITH_FILENAME) != 0)
		{
			rc = 1;
		}
	}
	
	return rc;
}