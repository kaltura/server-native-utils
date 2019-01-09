#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include "compressed_file.h"
#include "common.h"

bool_t
compressed_file_init(compressed_file_state_t* state, const char* file_name)
{
	const char* colon_pos;
	FILE* file;
	char* file_name_copy;
	long file_name_len;
	long start;
	long end;

	// parse range specification
	colon_pos = strchr(file_name, ':');
	if (colon_pos != NULL)
	{
		if (sscanf(colon_pos + 1, "%ld-%ld", &start, &end) != 2)
		{
			error(0, "failed to parse range specification %s", colon_pos);
			return FALSE;
		}

		file_name_len = colon_pos - file_name;
	}
	else
	{
		start = end = 0;
		file_name_len = strlen(file_name);
	}

	// strip the range specification
	file_name_copy = malloc(file_name_len + 1);
	if (file_name_copy == NULL)
	{
		error(0, "malloc failed");
		return FALSE;
	}

	memcpy(file_name_copy, file_name, file_name_len);
	file_name_copy[file_name_len] = '\0';

	// open the file and seek
	file = fopen(file_name_copy, "rb");
	if (file == NULL)
	{
		error(errno, "%s", file_name_copy);
		free(file_name_copy);
		return FALSE;
	}
	
	if (fseek(file, start, SEEK_SET) == -1)
	{
		error(errno, "%s", file_name_copy);
		fclose(file);
		free(file_name_copy);
		return FALSE;
	}

	// initialize the state
	state->source = file;
	state->file_name = file_name_copy;
	state->limit = end;
	state->strm.zalloc = Z_NULL;
	state->strm.zfree = Z_NULL;
	state->strm.opaque = Z_NULL;
	state->strm.avail_in = 0;
	state->strm.next_in = Z_NULL;
	return TRUE;
}

void
compressed_file_free(compressed_file_state_t* state)
{
	if (state->source != NULL)
	{
		fclose(state->source);
		state->source = NULL;
	}
	free(state->file_name);
	state->file_name = NULL;
}

long
compressed_file_get_pos(compressed_file_state_t* state)
{
	return ftell(state->source) - state->strm.avail_in;
}

int
compressed_file_process_segment(compressed_file_state_t* state, process_chunk_callback_t callback, void* context)
{
	int rc;

	rc = inflateInit2(&state->strm, 31);
	if (rc != Z_OK)
	{
		error(0, "inflateInit2 failed %d", rc);
		return PROCESS_ERROR;
	}

	do 
	{
		if (state->strm.avail_in == 0)
		{
			// read more data from file
			state->strm.avail_in = fread(state->in, 1, sizeof(state->in), state->source);
			if (ferror(state->source))
			{
				error(errno, "%s", state->file_name);
				(void)inflateEnd(&state->strm);
				return PROCESS_ERROR;
			}

			if (state->strm.avail_in == 0)
			{
				error(0, "%s: truncated file", state->file_name);
				(void)inflateEnd(&state->strm);
				return PROCESS_ERROR;
			}
			state->strm.next_in = state->in;
		}

		// inflate data until more input is needed / end of stream
		do 
		{
			state->strm.next_out = state->out;
			state->strm.avail_out = sizeof(state->out);
			rc = inflate(&state->strm, Z_NO_FLUSH);
			if (rc == Z_STREAM_ERROR)
			{
				// unexpected
				error(0, "%s: stream error", state->file_name);
				exit(1);
			}

			switch (rc) 
			{
			case Z_NEED_DICT:
				rc = Z_DATA_ERROR;
				// fallthrough

			case Z_DATA_ERROR:
			case Z_MEM_ERROR:
				(void)inflateEnd(&state->strm);
				return PROCESS_RESYNC;
			}

			callback(context, state->out, sizeof(state->out) - state->strm.avail_out);
		} while (state->strm.avail_out == 0);

	} while (rc != Z_STREAM_END);

	(void)inflateEnd(&state->strm);

	if (state->limit > 0)
	{
		if (compressed_file_get_pos(state) >= state->limit)
		{
			return PROCESS_DONE;
		}
	}
	else if (feof(state->source) && state->strm.avail_in <= 0)
	{
		return PROCESS_DONE;
	}

	return PROCESS_SUCCESS;
}

int
compressed_file_resync(compressed_file_state_t* state)
{
	if (state->strm.avail_in > 0)
	{
		// advance one byte to make sure we don't get stuck in endless loop
		state->strm.next_in++;
		state->strm.avail_in--;
	}

	for (;;)
	{
		if (state->strm.avail_in < 2)
		{
			// need more input
			if (state->strm.avail_in > 0)
			{
				// copy the one byte we have to the beginning of the buffer
				state->in[0] = state->strm.next_in[0];
				state->strm.avail_in = 1;
			}
			else
			{
				state->strm.avail_in = 0;
			}
			state->strm.next_in = state->in;

			// read from file
			state->strm.avail_in += fread(state->in + state->strm.avail_in, 1, sizeof(state->in) - state->strm.avail_in, state->source);
			if (ferror(state->source))
			{
				break;
			}

			if (state->strm.avail_in < 2)
			{
				break;
			}
		}

		// look for gzip marker
		if (state->strm.next_in[0] == 0x1f && state->strm.next_in[1] == 0x8b)
		{
			return 1;
		}

		state->strm.next_in++;
		state->strm.avail_in--;
	}

	return 0;
}
