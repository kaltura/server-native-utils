#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include "compressed_file.h"
#include "common.h"


enum {
	STATE_INFLATE,
	STATE_END,
	STATE_RESYNC,
};


static long
compressed_file_get_pos(compressed_file_state_t* state)
{
	return state->cur_pos - state->strm.avail_in;
}

static bool_t
compressed_file_inflate(compressed_file_state_t* state)
{
	int rc;

	while (state->strm.avail_in > 0)
	{
		state->state = STATE_INFLATE;

		// inflate data until more input is needed / end of stream
		do
		{
			state->strm.next_out = state->out;
			state->strm.avail_out = sizeof(state->out);
			rc = inflate(&state->strm, Z_NO_FLUSH);
			if (rc == Z_STREAM_ERROR)
			{
				// unexpected
				error(0, "%s: stream error", state->input_url);
				exit(1);
			}

			state->observer.process_chunk(state->context, state->out, sizeof(state->out) - state->strm.avail_out);

			switch (rc)
			{
			case Z_NEED_DICT:
			case Z_DATA_ERROR:
			case Z_MEM_ERROR:
				if (state->observer.segment_end)
				{
					state->observer.segment_end(state->context, compressed_file_get_pos(state), TRUE);
				}

				state->state = STATE_RESYNC;
				state->last_word = 0;

				return TRUE;
			}
		} while (state->strm.avail_out == 0);

		if (rc == Z_STREAM_END)
		{
			if (state->observer.segment_end)
			{
				state->observer.segment_end(state->context, compressed_file_get_pos(state), FALSE);
			}

			state->state = STATE_END;

			rc = inflateReset(&state->strm);
			if (rc != Z_OK)
			{
				error(0, "inflateReset failed %d", rc);
				return FALSE;
			}
		}
	}

	return TRUE;
}

static bool_t
compressed_file_resync(compressed_file_state_t* state)
{
	unsigned short sync_word = 0x8b1f;
	unsigned char* saved_next_in;
	unsigned char cur_byte;
	unsigned saved_avail_in;
	int rc;

	while (state->strm.avail_in > 0)
	{
		cur_byte = *state->strm.next_in++;
		state->strm.avail_in--;

		state->last_word = (cur_byte << 8) | (state->last_word >> 8);

		if (state->last_word != sync_word)
		{
			continue;
		}

		state->state = STATE_INFLATE;

		rc = inflateReset(&state->strm);
		if (rc != Z_OK)
		{
			error(0, "inflateReset failed %d", rc);
			return FALSE;
		}

		saved_next_in = state->strm.next_in;
		saved_avail_in = state->strm.avail_in;

		state->strm.next_in = (void*)&sync_word;
		state->strm.avail_in = sizeof(sync_word);
		state->strm.next_out = state->out;
		state->strm.avail_out = sizeof(state->out);
		rc = inflate(&state->strm, Z_NO_FLUSH);
		if (rc != Z_OK)
		{
			error(0, "inflate failed %d", rc);
			return FALSE;
		}

		state->strm.next_in = saved_next_in;
		state->strm.avail_in = saved_avail_in;

		if (state->observer.resync)
		{
			state->observer.resync(state->context, compressed_file_get_pos(state) - sizeof(sync_word));
		}

		break;
	}

	return TRUE;
}

static size_t
compressed_file_handle_data(void* buf, size_t mbr_size, size_t mbr_count, void* data)
{
	compressed_file_state_t* state = data;
	size_t size = mbr_size * mbr_count;

	state->cur_pos += size;

	state->strm.next_in = buf;
	state->strm.avail_in = size;

	while (state->strm.avail_in > 0)
	{
		switch (state->state)
		{
		case STATE_INFLATE:
		case STATE_END:
			if (!compressed_file_inflate(state))
			{
				return 0;
			}
			break;

		case STATE_RESYNC:
			if (!compressed_file_resync(state))
			{
				return 0;
			}
			break;
		}
	}

	return size;
}

long
compressed_file_init(compressed_file_state_t* state, curl_ext_conf_t* conf, const char* url, compressed_file_observer_t* observer, void* context)
{
	const char* range_start;
	const char* scheme_end;
	const char* prefix;
	CURLcode res;
	str_t url_str;
	char range[64];
	char* url_copy;
	long prefix_len;
	long url_len;
	long start;
	long end;
	int rc;

	memset(state, 0, offsetof(compressed_file_state_t, out));

	scheme_end = strstr(url, "://");
	if (scheme_end != NULL)
	{
		scheme_end += sizeof("://") - 1;
		prefix = "";
		prefix_len = 0;
	}
	else
	{
		scheme_end = url;
		prefix = "file://localhost";
		prefix_len = sizeof("file://localhost") - 1;
	}


	range_start = strchr(scheme_end, ':');
	if (range_start != NULL)
	{
		if (sscanf(range_start + 1, "%ld-%ld", &start, &end) != 2)
		{
			error(0, "failed to parse range specification %s", range_start);
			goto failed;
		}
	}
	else
	{
		start = end = 0;
		range_start = url + strlen(url);
	}


	url_len = prefix_len + range_start - url;

	state->curl = curl_easy_init();
	if (!state->curl)
	{
		error(0, "curl_easy_init failed");
		goto failed;
	}

	url_str.data = (char*)url;
	url_str.len = range_start - url;

	if (!curl_ext_ctx_init(&state->curl_ext, conf, &url_str, state->curl))
	{
		goto failed;
	}

	if (state->curl_ext.ctx == NULL)
	{
		// no curl extension - set the url as is
		url_copy = malloc(url_len + 1);
		if (url_copy == NULL)
		{
			error(0, "malloc failed");
			goto failed;
		}

		memcpy(url_copy, prefix, prefix_len);
		memcpy(url_copy + prefix_len, url, range_start - url);
		url_copy[url_len] = '\0';

		state->url = url_copy;

		res = curl_easy_setopt(state->curl, CURLOPT_URL, url_copy);
		if (res != CURLE_OK)
		{
			error(0, "curl_easy_setopt(CURLOPT_URL) failed %d", res);
			goto failed;
		}
	}

	res = curl_easy_setopt(state->curl, CURLOPT_WRITEFUNCTION, compressed_file_handle_data);
	if (res != CURLE_OK)
	{
		error(0, "curl_easy_setopt(CURLOPT_WRITEFUNCTION) failed %d", res);
		goto failed;
	}

	res = curl_easy_setopt(state->curl, CURLOPT_WRITEDATA, state);
	if (res != CURLE_OK)
	{
		error(0, "curl_easy_setopt(CURLOPT_WRITEDATA) failed %d", res);
		goto failed;
	}

	if (end)
	{
		sprintf(range, "%ld-%ld", start, end - 1);

		res = curl_easy_setopt(state->curl, CURLOPT_RANGE, range);
		if (res != CURLE_OK)
		{
			error(0, "curl_easy_setopt(CURLOPT_RANGE) failed %d", res);
			goto failed;
		}
	}

	state->input_url = strdup(url);
	if (state->input_url == NULL)
	{
		error(0, "strdup failed");
		goto failed;
	}

	rc = inflateInit2(&state->strm, 31);
	if (rc != Z_OK)
	{
		error(0, "inflateInit2 failed %d", rc);
		goto failed;
	}

	state->observer = *observer;
	state->context = context;
	state->cur_pos = start;

	return compressed_file_get_pos(state);

failed:

	compressed_file_free(state);

	return -1;
}

void
compressed_file_free(compressed_file_state_t* state)
{
	inflateEnd(&state->strm);

	free(state->input_url);
	state->input_url = NULL;

	free(state->url);
	state->url = NULL;

	curl_ext_ctx_free(&state->curl_ext);

	curl_easy_cleanup(state->curl);
	state->curl = NULL;
}

bool_t
compressed_file_process(compressed_file_state_t* state)
{
	CURLcode res;
	long code;
	long pos;

	res = curl_easy_perform(state->curl);
	if (res != CURLE_OK)
	{
		if (res != CURLE_WRITE_ERROR)
		{
			error(0, "%s: curl error %d - %s", state->input_url, res, curl_easy_strerror(res));
		}
		return FALSE;
	}

	res = curl_easy_getinfo(state->curl, CURLINFO_RESPONSE_CODE, &code);
	if (res != CURLE_OK)
	{
		error(0, "curl_easy_getinfo(CURLINFO_RESPONSE_CODE) failed %d", res);
		return FALSE;
	}

	if (code != 0 && (code < 200 || code >= 300))
	{
		error(0, "invalid status code %ld", code);
		return FALSE;
	}

	pos = compressed_file_get_pos(state);
	if (state->state == STATE_INFLATE && pos > 0)
	{
		error(0, "%s: truncated file", state->input_url);

		if (state->observer.segment_end)
		{
			state->observer.segment_end(state->context, pos, TRUE);
		}
	}

	return TRUE;
}
