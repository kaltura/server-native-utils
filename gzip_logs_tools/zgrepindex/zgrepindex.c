#define _XOPEN_SOURCE 700		// required for strptime
typedef unsigned char u_char;

#include <getopt.h>
#include <string.h>
#include <time.h>
#include <pcre.h>
#include "../compressed_file.h"
#include "../common.h"

// constants
#define MAX_LINE_SIZE (1024)
#define MAX_CAPTURE_SIZE (1024)
#define MIN_SEGMENT_SIZE (524288)		// TODO: add cmdline switch
#define MAX_CAPTURES (9)	// $1..$9

// enum
enum { 
	EXIT_ERROR = 2,
};

// typedefs
typedef struct {
	// line
	u_char line_buffer[MAX_LINE_SIZE];
	size_t line_buffer_size;
	bool_t line_start;

	// captures (null terminated)
	u_char last_value[MAX_CAPTURE_SIZE];
	size_t last_value_size;
	u_char min_value[MAX_CAPTURE_SIZE];
	size_t min_value_size;
	u_char max_value[MAX_CAPTURE_SIZE];
	size_t max_value_size;
} line_processor_state_t;

typedef struct {
	pcre *code;
	pcre_extra *extra;
} regex_t;

typedef struct {
	int capture_index;
	const u_char* data;
	size_t len;
} capture_expression_t;

// constants
static char const short_options[] = "p:t:c:";
static struct option const long_options[] =
{
	{"pattern", required_argument, NULL, 'p'},
	{"time-format", required_argument, NULL, 't'},
	{"capture-expression", required_argument, NULL, 'c'},
	{0, 0, 0, 0}
};

// globals
static int show_help = 0;
static const char* file_name;
static const char* time_format = NULL;
static regex_t regex;

static capture_expression_t* capture_expression = NULL;
static capture_expression_t default_capture_expression[2];
static int max_capture_index;

static void
usage(int status)
{
	if (status != 0)
	{
		fprintf (stderr, "Usage: %s [OPTION]... [FILE]...\n", program_name);
		fprintf (stderr, "Try '%s --help' for more information.\n", program_name);
	}
	else
    {
      printf ("Usage: %s [OPTION]... [FILE]...\n", program_name);
      printf ("\
Creates an index of gzip chunks for the given files.\n");
      printf ("\
Example: %s -p '(\\d{2}:\\d{2}:\\d{2})' input.log.gz\n\
\n", program_name);
		printf ("\
\n\
      --help                display this help text and exit\n\
  -p, --pattern             a regular expression that captures a timestamp\
                            from an input line.\n\
                            the default pattern is (.*)\n\
  -c, --capture-expression  an expression that is evaluated to get the\n\
                            timestamp of a given line. the default expression\n\
                            is '$1', meaning the first pattern capture.\n\
  -t, --time-format         parse the pattern match using the specified\n\
                            strptime format. when not provided, string\n\
                            comparison is used to compare timestamps\n\
");
	}
	exit(status);
}

static bool_t
parse_capture_expression(const char* str)
{
	capture_expression_t* cur;
	const char* next;
	const char* pos;
	int count;

	count = 0;
	for (pos = str; *pos; pos++)
	{
		if (*pos == '$')
		{
			count++;
		}
	}

	capture_expression = malloc(sizeof(capture_expression[0]) * (count + 1));
	if (capture_expression == NULL)
	{
		error(0, "malloc failed");
		return FALSE;
	}

	max_capture_index = -1;
	cur = capture_expression;
	pos = str;
	for (;;)
	{
		next = strchr(pos, '$');
		if (next == NULL)
		{
			cur->capture_index = -1;
			cur->data = (const u_char*)pos;
			cur->len = strlen(pos);
			break;
		}

		next++;
		if (*next < '1' || *next > '9')
		{
			error(0, "expected capture index in capture condition");
			free(capture_expression);
			capture_expression = NULL;
			return FALSE;
		}
		cur->capture_index = *next++ - '1';
		cur->data = (const u_char*)pos;
		cur->len = (next - 2) - pos;
		cur++;

		if (cur->capture_index > max_capture_index)
		{
			max_capture_index = cur->capture_index;
		}

		pos = next;
	}

	return TRUE;
}

static size_t
eval_capture_expression(u_char* dest, size_t dest_size, const u_char* buffer, int* captures)
{
	capture_expression_t* cur;
	const u_char* value;
	u_char* initial_dest = dest;
	u_char* dest_end;
	size_t value_len;
	size_t copy_size;
	int value_start;

	// Note: assuming dest_size > 0
	dest_end = dest + dest_size - 1;	// leave room for null

	for (cur = capture_expression; ; cur++)
	{
		// copy the fixed string
		copy_size = min(dest_end - dest, cur->len);
		memcpy(dest, cur->data, copy_size);
		dest += copy_size;

		if (cur->capture_index < 0)
		{
			break;
		}

		// copy the capture value
		value_start = captures[(cur->capture_index + 1) * 2];
		value_len = captures[(cur->capture_index + 1) * 2 + 1] - value_start;
		value = buffer + value_start;

		copy_size = min(dest_end - dest, value_len);
		memcpy(dest, value, value_len);
		dest += value_len;
	}

	*dest = '\0';

	return dest - initial_dest;
}

static int 
compare_timestamps(const void *s1, size_t n1, const void *s2, size_t n2)
{
	struct tm tm;
	time_t t1;
	time_t t2;

	// parse the times
	if (strptime(s1, time_format, &tm) == NULL)
	{
		error(0, "failed to parse timestamp \"%s\"", s1);
		t1 = 0;
	}
	else 
	{
		tm.tm_isdst = 0;		// not set by strptime
		t1 = mktime(&tm);
	}

	if (strptime(s2, time_format, &tm) == NULL)
	{
		error(0, "failed to parse timestamp \"%s\"", s2);
		t2 = 0;
	}
	else
	{
		tm.tm_isdst = 0;		// not set by strptime
		t2 = mktime(&tm);
	}

	return (int)t1 - (int)t2;
}

static int 
compare_matches(const void *s1, size_t n1, const void *s2, size_t n2)
{
	if (time_format != NULL)
	{
		return compare_timestamps(s1, n1, s2, n2);
	}

	return memcmp(s1, s2, min(n1, n2));
}

/// lines processor
static void
line_processor_reset(line_processor_state_t* state, bool_t line_start)
{
	state->line_start = line_start;
	state->line_buffer_size = 0;

	state->last_value_size = 0;
	state->min_value_size = 0;
	state->max_value_size = 0;
}

static void
line_processor_next_segment(line_processor_state_t* state)
{
	memcpy(state->min_value, state->last_value, state->last_value_size + 1);	// copy the null
	state->min_value_size = state->last_value_size;
	memcpy(state->max_value, state->last_value, state->last_value_size + 1);	// copy the null
	state->max_value_size = state->last_value_size;
}

static void
line_processor_print(line_processor_state_t* state, long start_offset, long end_offset)
{
	if (state->min_value_size <= 0 || state->max_value_size <= 0)
	{
		return;
	}

	printf("%ld\t%ld\t%s\t%s\n", start_offset, end_offset, state->min_value, state->max_value);
}

static void
line_processor_process(void* context, u_char* pos, size_t size)
{
	line_processor_state_t* state = context;
	size_t copy_size;
	u_char* cur_end;
	u_char* newline;
	u_char* end;
	u_char* line_buffer;
	size_t line_size;
	int captures[(1 + MAX_CAPTURES) * 3];
	int exec_result;

	for (end = pos + size; pos < end; pos = cur_end)
	{
		// find a newline
		newline = memchr(pos, '\n', end - pos);
		cur_end = newline != NULL ? newline + 1 : end;

		if (!state->line_start)
		{
			// ignore all data until a newline is found
			if (newline != NULL)
			{
				state->line_start = TRUE;
			}
			continue;
		}

		if (state->line_buffer_size > 0 || 
			(newline == NULL && cur_end - pos < sizeof(state->line_buffer)))
		{
			// copy as much as possible to the buffer
			copy_size = min(cur_end - pos, sizeof(state->line_buffer) - state->line_buffer_size);
			memcpy(state->line_buffer + state->line_buffer_size, pos, copy_size);
			state->line_buffer_size += copy_size;
			pos += copy_size;
		}

		if (newline == NULL)
		{
			if (state->line_buffer_size < sizeof(state->line_buffer))
			{
				// wait for more data
				break;
			}

			// after processing the buffer, ignore all data until a newline
			state->line_start = FALSE;
		}

		// match the line against the regex
		if (state->line_buffer_size > 0)
		{
			line_buffer = state->line_buffer;
			line_size = state->line_buffer_size;
			state->line_buffer_size = 0;
		}
		else
		{
			line_buffer = pos;
			line_size = cur_end - pos;
		}

		exec_result = pcre_exec(
			regex.code, 
			regex.extra, 
			(const char *)line_buffer, 
			line_size, 
			0, 
			0, 
			captures, 
			sizeof(captures) / sizeof(captures[0]));

		if (exec_result < max_capture_index + 2)
		{
			// no match / some error
			continue;
		}

		// copy to last value
		state->last_value_size = eval_capture_expression(
			state->last_value, 
			sizeof(state->last_value), 
			line_buffer, 
			captures);

		// update min/max value
		if (state->min_value_size == 0 ||
			compare_matches(state->last_value, state->last_value_size, state->min_value, state->min_value_size) < 0)
		{
			memcpy(state->min_value, state->last_value, state->last_value_size + 1);	// copy the null
			state->min_value_size = state->last_value_size;
		}

		if (state->max_value_size == 0 ||
			compare_matches(state->last_value, state->last_value_size, state->max_value, state->max_value_size) > 0)
		{
			memcpy(state->max_value, state->last_value, state->last_value_size + 1);	// copy the null
			state->max_value_size = state->last_value_size;
		}
	}
}

/// main
static int 
process_file()
{
	compressed_file_state_t file_state;
	line_processor_state_t lines_state;
	long segment_start;
	long cur_pos;
	int result = 1;
	int status;

	// initialize
	if (!compressed_file_init(&file_state, file_name))
	{
		return 1;
	}

	segment_start = compressed_file_get_pos(&file_state);

	line_processor_reset(&lines_state, segment_start == 0);

	for (;;)
	{
		// process a segment
		status = compressed_file_process_segment(&file_state, &line_processor_process, &lines_state);

		// print the segment info
		cur_pos = compressed_file_get_pos(&file_state);
		if (cur_pos - segment_start < MIN_SEGMENT_SIZE && status == PROCESS_SUCCESS)
		{
			continue;
		}

		line_processor_print(&lines_state, segment_start, cur_pos);

		switch (status)
		{
		case PROCESS_DONE:
			result = 0;
			// fallthrough

		case PROCESS_ERROR:
			goto done;

		case PROCESS_SUCCESS:
			// move to the next segment
			line_processor_next_segment(&lines_state);
			segment_start = cur_pos;
			continue;

		case PROCESS_RESYNC:
			break;		// handled outside the switch
		}

		// resync
		if (!compressed_file_resync(&file_state))
		{
			error(0, "%s: data error, didn't find start marker", file_name);
			break;
		}

		line_processor_reset(&lines_state, FALSE);

		segment_start = compressed_file_get_pos(&file_state);
		error(0, "%s: data error, trying to resync at %ld", file_name, segment_start);
	}

done:
	compressed_file_free(&file_state);
	return result;
}

int 
main(int argc, char **argv)
{
	const char *errstr;
	char* pattern = "(.*)";
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
			case 'p':
				pattern = optarg;
				break;

			case 'c':
				if (!parse_capture_expression(optarg))
				{
					return EXIT_ERROR;
				}
				break;

			case 't':
				time_format = optarg;
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

	if (optind + 1 > argc)
    {
		usage(EXIT_SUCCESS);
	}

	if (capture_expression == NULL)
	{
		capture_expression = default_capture_expression;
		memset(default_capture_expression, 0, sizeof(default_capture_expression));
		default_capture_expression[1].capture_index = -1;
		max_capture_index = 0;
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
		if (process_file() != 0)
		{
			rc = 1;
		}
	}

	return rc;
}
