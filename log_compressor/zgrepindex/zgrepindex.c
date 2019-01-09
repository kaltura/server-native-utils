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
static char const short_options[] = "p:t:";
static struct option const long_options[] =
{
	{"pattern", required_argument, NULL, 'p'},
	{"time-format", required_argument, NULL, 't'},
	{0, 0, 0, 0}
};

// globals
static int show_help = 0;
static const char* file_name;
static const char* time_format = NULL;
static regex_t regex;

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
		t1 = mktime(&tm);
	}

	if (strptime(s2, time_format, &tm) == NULL)
	{
		error(0, "failed to parse timestamp \"%s\"", s2);
		t2 = 0;
	}
	else
	{
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
	int captures[6];
	
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
		
		if (pcre_exec(
			regex.code, 
			regex.extra, 
			(const char *)line_buffer, 
			line_size, 
			0, 
			0, 
			captures, 
			sizeof(captures) / sizeof(captures[0])) < 2)
		{
			// no match / some error
			continue;
		}
	
		// copy to last value
		state->last_value_size = captures[3] - captures[2];
		if (state->last_value_size > sizeof(state->last_value) - 1)
		{
			state->last_value_size = sizeof(state->last_value) - 1;
		}
		memcpy(state->last_value, line_buffer + captures[2], state->last_value_size);
		state->last_value[state->last_value_size] = '\0';

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
		if (status == PROCESS_ERROR)
		{
			break;
		}

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
