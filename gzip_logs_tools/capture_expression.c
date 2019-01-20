#include <string.h>
#include "capture_expression.h"
#include "common.h"

capture_expression_t*
parse_capture_expression(
	const char* str,
	size_t len,
	int* max_capture_index)
{
	capture_expression_t* result;
	capture_expression_t* cur;
	const char* next;
	const char* pos;
	const char* end = str + len;
	int count;

	count = 0;
	for (pos = str; pos < end; pos++)
	{
		if (*pos == '$')
		{
			count++;
		}
	}

	result = malloc(sizeof(result[0]) * (count + 1));
	if (result == NULL)
	{
		error(0, "malloc failed");
		return NULL;
	}

	*max_capture_index = -1;
	cur = result;
	pos = str;
	for (;;)
	{
		next = memchr(pos, '$', end - pos);
		if (next == NULL)
		{
			cur->capture_index = -1;
			cur->data = pos;
			cur->len = end - pos;
			break;
		}

		next++;
		if (next >= end || *next < '1' || *next > '9')
		{
			error(0, "expected capture index in capture condition");
			free(result);
			return NULL;
		}
		cur->capture_index = *next++ - '1';
		cur->data = pos;
		cur->len = (next - 2) - pos;
		cur++;

		if (cur->capture_index > *max_capture_index)
		{
			*max_capture_index = cur->capture_index;
		}

		pos = next;
	}

	return result;
}

size_t
eval_capture_expression(
	capture_expression_t* capture_expression,
	char* dest,
	size_t dest_size,
	const char* buffer,
	int* captures)
{
	capture_expression_t* cur;
	const char* value;
	char* initial_dest = dest;
	char* dest_end;
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
		memcpy(dest, value, copy_size);
		dest += copy_size;
	}

	*dest = '\0';

	return dest - initial_dest;
}
