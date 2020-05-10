#define _XOPEN_SOURCE 700		// required for strptime
typedef unsigned char u_char;

#include <getopt.h>
#include <string.h>
#include <time.h>
#include <pcre.h>
#include "../capture_expression.h"
#include "../compressed_file.h"
#include "../common.h"

// constants
#define MAX_LINE_SIZE (1024)
#define MAX_CAPTURE_SIZE (1024)
#define MIN_SEGMENT_SIZE (524288)		// TODO: add cmdline switch

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
			capture_expression,
			(char*)state->last_value,
			sizeof(state->last_value),
			(char*)line_buffer,
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
typedef struct {
	line_processor_state_t lines;
	compressed_file_state_t file;
	long segment_start;
} index_state_t;

static void
index_resync(void* context, long pos)
{
	index_state_t* state = context;

	state->segment_start = pos;
	error(0, "%s: data error, trying to resync at %ld", file_name, pos);
}

static void
index_segment_end(void* context, long pos, bool_t error)
{
	index_state_t* state = context;

	if (pos - state->segment_start < MIN_SEGMENT_SIZE && !error)
	{
		return;
	}

	line_processor_print(&state->lines, state->segment_start, pos);

	if (error)
	{
		line_processor_reset(&state->lines, FALSE);
	}
	else
	{
		line_processor_next_segment(&state->lines);
		state->segment_start = pos;
	}
}

static int
process_file()
{
	compressed_file_observer_t observer;
	index_state_t state;
	int result = 1;

	// initialize
	observer.process_chunk = line_processor_process;
	observer.resync = index_resync;
	observer.segment_end = index_segment_end;

	state.segment_start = compressed_file_init(&state.file, file_name, &observer, &state);
	if (state.segment_start < 0)
	{
		return 1;
	}

	line_processor_reset(&state.lines, state.segment_start == 0);

	if (compressed_file_process(&state.file))
	{
		result = 0;
	}

	compressed_file_free(&state.file);
	return result;
}

int
main(int argc, char **argv)
{
	const char *errstr;
	CURLcode res;
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
				capture_expression = parse_capture_expression(
					optarg,
					strlen(optarg),
					&max_capture_index);
				if (capture_expression == NULL)
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

		return EXIT_ERROR;
	}

	regex.extra = pcre_study(regex.code, 0, &errstr);
	if (errstr != NULL)
	{
		error(0, "pcre_study() failed: %s", errstr);
	}


	// init curl
	res = curl_global_init(CURL_GLOBAL_DEFAULT);
	if (res != CURLE_OK)
	{
		error(0, "curl_global_init failed: %d", res);
		return EXIT_ERROR;
	}

	// process the files
	rc = EXIT_SUCCESS;
	while (optind < argc)
	{
		file_name = argv[optind++];
		if (process_file() != 0)
		{
			rc = EXIT_ERROR;
		}
	}

	curl_global_cleanup();

	return rc;
}
