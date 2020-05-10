#define _XOPEN_SOURCE 700		// required for strptime
typedef unsigned char u_char;

#include <getopt.h>
#include <string.h>
#include <time.h>
#include <pcre.h>
#include "../compressed_file.h"
#include "../capture_expression.h"
#include "filter.h"

// enums
enum {
	PM_UNDEFINED,
	PM_NO_FILENAME,
	PM_WITH_FILENAME,
};

enum {
	EXIT_ERROR = 2,
};

// typedefs
typedef struct {
	pcre *code;
	pcre_extra *extra;
} regex_t;

// globals
static int show_help = 0;

static regex_t regex;
static filter_base_t* filter = NULL;
static char* block_delimiter = NULL;
static size_t block_delimiter_len = 0;
static const char* time_format = "%Y-%m-%d %H:%M:%S";

// constants
static char const short_options[] = "p:t:c:f:d:hH";
static struct option const long_options[] =
{
	{"pattern", required_argument, NULL, 'p'},
	{"time-format", required_argument, NULL, 't'},
	{"capture-conditions", required_argument, NULL, 'c'},
	{"filter", required_argument, NULL, 'f'},
	{"block-delimiter", required_argument, NULL, 'd'},
	{"no-filename", no_argument, NULL, 'h'},
	{"with-filename", no_argument, NULL, 'H'},
	{"help", no_argument, &show_help, 1},
	{0, 0, 0, 0}
};

/// capture conditions
enum {
	COMPARE_TYPE_STRING,
	COMPARE_TYPE_GENERAL_NUM,
	COMPARE_TYPE_TIME,
};

enum {
	CONDITION_NONE,
	CONDITION_EQUALS,
	CONDITION_LESS_THAN,
	CONDITION_LESS_EQUAL,
	CONDITION_GREATER_THAN,
	CONDITION_GREATER_EQUAL,
};

typedef struct capture_condition_s capture_condition_t;

typedef int (*compare_func_t)(capture_condition_t* cond, const char* str, size_t len);

struct capture_condition_s {
	capture_expression_t* capture_expression;
	int capture_index;
	int comparison;
	compare_func_t comparison_func;
	union {
		struct {
			const char* data;
			size_t len;
		} str;
		long double num;
		time_t time;
	} u;
};

static capture_condition_t* capture_conditions = NULL;

static int
string_compare(capture_condition_t* cond, const char* str, size_t len)
{
	int compare_result;

	compare_result = memcmp(str, cond->u.str.data, min(len, cond->u.str.len));
	if (compare_result != 0)
	{
		return compare_result;
	}

	if (len < cond->u.str.len)
	{
		return -1;
	}
	else if (len > cond->u.str.len)
	{
		return 1;
	}

	return 0;
}

static bool_t
parse_time(const char* str, size_t len, time_t* result)
{
	char buf[128];
	struct tm tm;

	// null terminate
	if (len > sizeof(buf) - 1)
	{
		len = sizeof(buf) - 1;
	}
	memcpy(buf, str, len);
	buf[len] = '\0';

	// parse the time
	if (strptime(str, time_format, &tm) == NULL)
	{
		return FALSE;
	}

	tm.tm_isdst = 0;		// not set by strptime
	*result = mktime(&tm);
	return TRUE;
}

static int
time_compare(capture_condition_t* cond, const char* str, size_t len)
{
	time_t time;

	if (!parse_time(str, len, &time))
	{
		return -1;
	}

	return (int)time - (int)cond->u.time;
}

static bool_t
parse_general_num(const char* str, size_t len, long double* result)
{
	char buf[32];
	char* endptr;

	// null terminate
	if (len > sizeof(buf) - 1)
	{
		len = sizeof(buf) - 1;
	}
	memcpy(buf, str, len);
	buf[len] = '\0';

	*result = strtold(buf, &endptr);

	return endptr != buf;
}

static int
general_numcompare(capture_condition_t* cond, const char* str, size_t len)
{
	long double num;

	// the code below taken from coreutils sort - general_numcompare

	// put conversion errors at the start of the collating sequence.
	if (!parse_general_num(str, len, &num))
	{
		return -1;
	}

	// Sort numbers in the usual way, where -0 == +0.  Put NaNs after
	//	conversion errors but before numbers; sort them by internal
	//	bit-pattern, for lack of a more portable alternative.
	return (num < cond->u.num ? -1
		: num > cond->u.num ? 1
		: num == cond->u.num ? 0
		: cond->u.num == cond->u.num ? -1
		: num == num ? 1
		: memcmp(&num, &cond->u.num, sizeof(num)));
}

static capture_condition_t*
capture_conditions_parse(const char* str)
{
	capture_condition_t* result;
	capture_condition_t* cur;
	const char* pos;
	const char* end_pos;
	const char* comma_pos;
	const char* value;
	size_t value_len;
	int compare_type;
	int count;

	count = 0;
	for (pos = str; *pos; pos++)
	{
		if (*pos == ',')
		{
			count++;
		}
	}

	result = malloc(sizeof(result[0]) * (count + 2));
	if (result == NULL)
	{
		error(0, "malloc failed");
		return NULL;
	}

	cur = result;
	pos = str;
	for (;;)
	{
		switch (*pos++)
		{
		case '$':
			if (*pos < '1' || *pos > '9')
			{
				error(0, "expected capture index in capture condition");
				goto error;
			}
			cur->capture_expression = NULL;
			cur->capture_index = *pos++ - '1';
			break;

		case '"':
		case '\'':
			end_pos = strchr(pos, pos[-1]);
			if (end_pos == NULL)
			{
				error(0, "missing closing quote");
				goto error;
			}

			cur->capture_expression = parse_capture_expression(
				pos,
				end_pos - pos,
				&cur->capture_index);
			if (cur->capture_expression == NULL)
			{
				goto error;
			}

			pos = end_pos + 1;
			break;

		default:
			error(0, "expected $ in capture condition");
			goto error;
		}

		switch (*pos)
		{
		case '@':
			compare_type = COMPARE_TYPE_TIME;
			pos++;
			break;

		case '#':
			compare_type = COMPARE_TYPE_GENERAL_NUM;
			pos++;
			break;

		default:
			compare_type = COMPARE_TYPE_STRING;
			break;
		}

		switch (*pos++)
		{
		case '=':
			cur->comparison = CONDITION_EQUALS;
			break;

		case '<':
			if (*pos == '=')
			{
				cur->comparison = CONDITION_LESS_EQUAL;
				pos++;
			}
			else
			{
				cur->comparison = CONDITION_LESS_THAN;
			}
			break;

		case '>':
			if (*pos == '=')
			{
				cur->comparison = CONDITION_GREATER_EQUAL;
				pos++;
			}
			else
			{
				cur->comparison = CONDITION_GREATER_THAN;
			}
			break;

		default:
			error(0, "expected comparison operator");
			goto error;
		}

		value = pos;
		comma_pos = strchr(pos, ',');
		if (comma_pos == NULL)
		{
			value_len = strlen(pos);
		}
		else
		{
			value_len = comma_pos - pos;
		}

		switch (compare_type)
		{
		case COMPARE_TYPE_TIME:
			if (!parse_time(value, value_len, &cur->u.time))
			{
				error(0, "failed to parse reference time");
				goto error;
			}
			cur->comparison_func = time_compare;
			break;

		case COMPARE_TYPE_GENERAL_NUM:
			if (!parse_general_num(value, value_len, &cur->u.num))
			{
				error(0, "failed to parse reference number");
				goto error;
			}
			cur->comparison_func = general_numcompare;
			break;

		case COMPARE_TYPE_STRING:
			cur->u.str.data = value;
			cur->u.str.len = value_len;
			cur->comparison_func = string_compare;
			break;
		}

		if (comma_pos == NULL)
		{
			break;
		}

		pos = comma_pos + 1;
		cur++;
	}

	cur[1].comparison = CONDITION_NONE;
	return result;

error:
	free(result);
	return NULL;
}

static bool_t
capture_conditions_eval(const char* buffer, int* captures, int exec_result)
{
	capture_condition_t* cur;
	const char* value;
	char buf[1024];
	size_t value_len;
	bool_t cur_result;
	int value_start;
	int compare_result;

	if (capture_conditions == NULL)
	{
		return TRUE;
	}

	for (cur = capture_conditions; cur->comparison != CONDITION_NONE; cur++)
	{
		if (exec_result < cur->capture_index + 2)
		{
			return FALSE;
		}

		if (cur->capture_expression != NULL)
		{
			value_len = eval_capture_expression(
				cur->capture_expression,
				buf,
				sizeof(buf),
				buffer,
				captures);
			value = buf;
		}
		else
		{
			value_start = captures[(cur->capture_index + 1) * 2];
			value_len = captures[(cur->capture_index + 1) * 2 + 1] - value_start;
			value = buffer + value_start;
		}

		// optimization for string '=' - no need to run compare if the lengths are different
		if (cur->comparison_func == string_compare &&
			cur->comparison == CONDITION_EQUALS)
		{
			if (value_len == cur->u.str.len &&
				memcmp(value, cur->u.str.data, value_len) == 0)
			{
				continue;
			}

			return FALSE;
		}

		compare_result = cur->comparison_func(cur, value, value_len);

		switch (cur->comparison)
		{
		case CONDITION_EQUALS:
			cur_result = compare_result == 0;
			break;

		case CONDITION_LESS_THAN:
			cur_result = compare_result < 0;
			break;

		case CONDITION_LESS_EQUAL:
			cur_result = compare_result <= 0;
			break;

		case CONDITION_GREATER_THAN:
			cur_result = compare_result > 0;
			break;

		case CONDITION_GREATER_EQUAL:
			cur_result = compare_result >= 0;
			break;

		default:
			cur_result = FALSE;
			break;
		}

		if (!cur_result)
		{
			return FALSE;
		}
	}

	return TRUE;
}

/// block processor
enum {
	STATE_IGNORE_BLOCK,
	STATE_OUTPUT_BLOCK,
	STATE_COLLECT_BLOCK,
};

typedef struct {
	int state;
	const char* prefix_data;
	size_t prefix_len;
	const char* suffix_data;
	size_t suffix_len;
	u_char block_buffer[10240];
	u_char* cur_block_start;
	u_char* cur_block_end;
} block_processor_state_t;

static void
block_processor_init(
	block_processor_state_t* state,
	const char* prefix_data,
	size_t prefix_len,
	const char* suffix_data,
	size_t suffix_len)
{
	state->state = STATE_IGNORE_BLOCK;
	state->prefix_data = prefix_data;
	state->prefix_len = prefix_len;
	state->suffix_data = suffix_data;
	state->suffix_len = suffix_len;
}

static bool_t
block_processor_eval_filter(block_processor_state_t* state)
{
	if (filter != NULL &&
		!filter_eval(filter, (char*)state->cur_block_start, state->cur_block_end - state->cur_block_start))
	{
		state->state = STATE_IGNORE_BLOCK;
		return FALSE;
	}

	state->state = STATE_OUTPUT_BLOCK;
	if (state->prefix_len != 0)
	{
		fwrite(state->prefix_data, state->prefix_len, 1, stdout);
	}
	fwrite(state->cur_block_start, state->cur_block_end - state->cur_block_start, 1, stdout);
	return TRUE;
}

static void
block_processor_line_start(block_processor_state_t* state, u_char* buffer, size_t size)
{
	int captures[(1 + MAX_CAPTURES) * 3];
	int exec_result;

	// run the regex to check for block start
	exec_result = pcre_exec(
		regex.code,
		regex.extra,
		(const char *)buffer,
		size,
		0,
		0,
		captures,
		sizeof(captures) / sizeof(captures[0]));
	if (exec_result < PCRE_ERROR_NOMATCH)
	{
		error(0, "pcre_exec failed %d", exec_result);
		return;
	}

	if (exec_result == PCRE_ERROR_NOMATCH)
	{
		// not a block start
		return;
	}

	if (state->state == STATE_COLLECT_BLOCK)
	{
		// evaluate the previous block
		block_processor_eval_filter(state);
	}

	if (state->state == STATE_OUTPUT_BLOCK && state->suffix_len > 0)
	{
		// write the suffix
		fwrite(state->suffix_data, state->suffix_len, 1, stdout);
	}

	if (!capture_conditions_eval((const char *)buffer, captures, exec_result))
	{
		state->state = STATE_IGNORE_BLOCK;
		return;
	}

	state->state = STATE_COLLECT_BLOCK;
	state->cur_block_start = NULL;
}

static void
block_processor_append_data(block_processor_state_t* state, u_char* buffer, size_t size)
{
	size_t copy_size;

	switch (state->state)
	{
	case STATE_IGNORE_BLOCK:
		return;

	case STATE_OUTPUT_BLOCK:
		fwrite(buffer, size, 1, stdout);
		return;

	case STATE_COLLECT_BLOCK:
		break;		// handled outside the switch
	}

	if (state->cur_block_start == NULL)
	{
		// no data, just point to the provided buffer
		state->cur_block_start = buffer;
		state->cur_block_end = buffer + size;
		return;
	}

	if (state->cur_block_end == buffer)
	{
		// contiguous - extend the current buffer
		state->cur_block_end += size;
		return;
	}

	if (state->cur_block_start != state->block_buffer)
	{
		// copy current block to the buffer
		copy_size = state->cur_block_end - state->cur_block_start;
		if (copy_size > sizeof(state->block_buffer))
		{
			// Note: this can't happen since the line buffer is smaller than the block buffer
			error(0, "unexpected - not enough room in block buffer");
			exit(1);
		}

		memcpy(state->block_buffer, state->cur_block_start, copy_size);
		state->cur_block_start = state->block_buffer;
		state->cur_block_end = state->block_buffer + copy_size;
	}

	// current block is kept in block buffer, copy as much as possible
	copy_size = min(size, state->block_buffer + sizeof(state->block_buffer) - state->cur_block_end);
	memcpy(state->cur_block_end, buffer, copy_size);
	state->cur_block_end += copy_size;

	if (state->cur_block_end < state->block_buffer + sizeof(state->block_buffer))
	{
		return;
	}

	// buffer is full, evaluate the block
	if (block_processor_eval_filter(state))
	{
		fwrite(buffer + copy_size, size - copy_size, 1, stdout);
	}
}

static void
block_processor_flush(block_processor_state_t* state)
{
	size_t size;

	if (state->state != STATE_COLLECT_BLOCK ||
		state->cur_block_start == NULL ||
		state->cur_block_start == state->block_buffer)
	{
		return;
	}

	size = state->cur_block_end - state->cur_block_start;
	if (size >= sizeof(state->block_buffer))
	{
		// cant buffer the block, evaluate it
		block_processor_eval_filter(state);
		return;
	}

	// save the block to the buffer
	memcpy(state->block_buffer, state->cur_block_start, size);
	state->cur_block_start = state->block_buffer;
	state->cur_block_end = state->block_buffer + size;
}

/// line processor
typedef struct {
	block_processor_state_t* block_state;
	u_char line_buffer[1024];
	size_t line_buffer_size;
	bool_t line_start;
} line_processor_state_t;

static void
line_processor_init(line_processor_state_t* state, block_processor_state_t* block_state, bool_t line_start)
{
	state->block_state = block_state;
	state->line_start = line_start;
	state->line_buffer_size = 0;
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

	for (end = pos + size; pos < end; pos = cur_end)
	{
		// find a newline
		newline = memchr(pos, '\n', end - pos);
		cur_end = newline != NULL ? newline + 1 : end;

		if (!state->line_start)
		{
			// ignore all data until a newline is found
			block_processor_append_data(state->block_state, pos, cur_end - pos);
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

		// call the block processor to handle the line data
		if (state->line_buffer_size > 0)
		{
			line_buffer = state->line_buffer;
			line_size = state->line_buffer_size;
		}
		else
		{
			line_buffer = pos;
			line_size = cur_end - pos;
		}

		block_processor_line_start(state->block_state, line_buffer, line_size);

		// pass all data up to cur_end to the block processor
		if (state->line_buffer_size > 0)
		{
			block_processor_append_data(state->block_state, line_buffer, line_size);
		}
		block_processor_append_data(state->block_state, pos, cur_end - pos);

		state->line_buffer_size = 0;
	}

	// flush the block processor (should not keep any pointers after this function returns)
	block_processor_flush(state->block_state);
}

/// main
static int
process_file(const char* file_name, int file_name_prefix)
{
	compressed_file_observer_t observer;
	compressed_file_state_t compressed_file_state;
	block_processor_state_t block_state;
	line_processor_state_t line_state;
	const char* colon_pos;
	size_t prefix_len;
	char* prefix_data = NULL;
	long file_pos;

	// open the file
	memset(&observer, 0, sizeof(observer));
	observer.process_chunk = &line_processor_process;

	file_pos = compressed_file_init(&compressed_file_state, file_name, &observer, &line_state);
	if (file_pos < 0)
	{
		return 1;
	}

	// initialize the prefix buffer
	if (file_name_prefix)
	{
		colon_pos = strrchr(file_name, ':');
		if (colon_pos != NULL && colon_pos[1] != '/')
		{
			prefix_len = colon_pos - file_name;
		}
		else
		{
			prefix_len = strlen(file_name);
		}

		prefix_data = malloc(prefix_len + 2);
		if (prefix_data == NULL)
		{
			error(0, "malloc failed");
			return 1;
		}

		memcpy(prefix_data, file_name, prefix_len);
		prefix_data[prefix_len++] = ':';
		prefix_data[prefix_len++] = ' ';
	}
	else
	{
		prefix_len = 0;
	}

	// initialize the state machines
	block_processor_init(&block_state, prefix_data, prefix_len, block_delimiter, block_delimiter_len);

	line_processor_init(&line_state, &block_state, file_pos == 0);

	compressed_file_process(&compressed_file_state);


	// clean up
	free(prefix_data);
	compressed_file_free(&compressed_file_state);
	return 0;
}

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
Search for blocks matching the provided search criteria in each gzip FILE.\n\
A block is a set of lines, the first of which matches a provided regular\n\
expression.\n\
\n\
Each FILE may contain a range specification of the format FILE:START-END,\n\
where START and END are byte offsets within the file.\n");

      printf ("\
Example: %s -p '(\\d{2}:\\d{2}:\\d{2})' -c '$1>=12:34:56' input.log.gz\n\
\n", program_name);

		printf ("\
\n\
      --help                display this help text and exit\n\
  -H, --with-filename       print the file name for each match\n\
  -h, --no-filename         suppress the file name prefix on output\n\
  -p, --pattern             a regular expression that identifies block start.\n\
                            the default pattern is ^.\n\
  -t, --time-format         sets the strptime format that should be used when\n\
                            performing timestamp comparisons. The default is -\n\
                            '%%Y-%%m-%%d %%H:%%M:%%S'\n\
  -c, --capture-conditions  an optional set of conditions on the pattern\n\
                            regular expression captures, more details below.\n\
  -f, --filter              an optional JSON encoded filter object that is\n\
                            matched against each block, more details below.\n\
  -d, --block-delimiter     a string that is printed in a separate line\n\
                            following each identified block.\n\
");

		printf ("\n\
Capture conditions:\n\
Each capture condition has 3 parts -\n\
- capture variable          $1 is the first capture, $2 the second, etc.\n\
- comparison operator       one of the following - =, <, >, <=, >=.\n\
                            adding # before the operator makes the operator\n\
                            treat both values as numeric. e.g. 10<5 is true,\n\
                            but 10#<5 is false.\n\
- reference value           the string that should be compared to the capture\n\
                            variable.\n\
\n\
Multiple capture conditions can be provided, by delimiting them with commas.\n\
\n\
Filter objects:\n\
A filter is a JSON object that is evaluated against each block, only blocks\n\
matching the filter are printed. Each filter has a 'type' property, the\n\
following types are supported:\n\
\n\
- match       simple text search, has the following properties -\n\
    text        string, the text that is searched within the block\n\
    ignorecase  optional boolean, default true\n\
\n\
- regex       regular expression match, has the following properties -\n\
    pattern     string, the regular expression pattern\n\
    ignorecase  optional boolean, see PCRE_CASELESS, true by default\n\
    multiline   optional boolean, see PCRE_MULTILINE\n\
    dotall      optional boolean, see PCRE_DOTALL\n\
    ungreedy    optional boolean, see PCRE_UNGREEDY\n\
\n\
- not         logical NOT operator, has the following properties -\n\
    filter      a filter object\n\
\n\
- and         logical AND operator, has the following properties -\n\
    filters     an array of filter objects\n\
\n\
- or          logical OR operator, has the following properties -\n\
    filters     an array of filter objects\n\
");
	}
	exit(status);
}

int
main(int argc, char **argv)
{
	const char *errstr;
	CURLcode res;
	char* pattern = "^.";
	char error_str[128];
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

		case 'p':
			pattern = optarg;
			break;

		case 't':
			time_format = optarg;
			break;

		case 'c':
			capture_conditions = capture_conditions_parse(optarg);
			if (capture_conditions == NULL)
			{
				return EXIT_ERROR;
			}
			break;

		case 'f':
			filter = filter_parse(optarg, error_str, sizeof(error_str));
			if (filter == NULL)
			{
				error(0, "failed to parse filter: %s", error_str);
				return EXIT_ERROR;
			}
			break;

		case 'd':
			block_delimiter_len = strlen(optarg);
			block_delimiter = malloc(block_delimiter_len + 1);
			if (block_delimiter == NULL)
			{
				error(0, "malloc failed");
				return EXIT_ERROR;
			}
			memcpy(block_delimiter, optarg, block_delimiter_len);
			block_delimiter[block_delimiter_len++] = '\n';
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
		if (process_file(argv[optind++], prefix_mode == PM_WITH_FILENAME) != 0)
		{
			rc = EXIT_ERROR;
		}
	}

	curl_global_cleanup();

	return rc;
}
