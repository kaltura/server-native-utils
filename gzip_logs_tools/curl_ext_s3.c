#include <string.h>
#include <openssl/sha.h>
#include <openssl/hmac.h>
#include "curl_ext_s3.h"


#define CURL_EXT_S3_SHA256_HEX_LEN	(SHA256_DIGEST_LENGTH * 2)
#define CURL_EXT_S3_HMAC_HEX_LEN	(EVP_MAX_MD_SIZE * 2)

#define CURL_EXT_S3_AMZ_TIME_FORMAT	("%Y%m%dT%H%M%SZ")
#define CURL_EXT_S3_AMZ_TIME_LEN	(sizeof("YYYYmmddTHHMMSSZ"))

#define CURL_EXT_S3_AMZ_DATE_FORMAT	("%Y%m%d")
#define CURL_EXT_S3_AMZ_DATE_LEN	(sizeof("YYYYmmdd"))


#define CURL_EXT_S3_EMPTY_SHA256	\
	"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

#define CURL_EXT_S3_SECURITY_TOKEN  ("X-Amz-Security-Token: ")

#define CURL_EXT_S3_SECURITY_TOKEN_CANON  ("x-amz-security-token:")

#define COMPESSED_FILE_S3_AMZ_DATE_HEADER "X-Amz-Date: %.*s"
#define COMPESSED_FILE_S3_AMZ_DATE_HEADER_LEN \
	sizeof(COMPESSED_FILE_S3_AMZ_DATE_HEADER) + CURL_EXT_S3_AMZ_TIME_LEN

#define COMPESSED_FILE_S3_HOST "%.*s.s3.%.*s.amazonaws.com"

#define COMPESSED_FILE_S3_SCHEME "https://"


typedef struct {
	str_t region;
	str_t access_key;
	str_t secret_key;
	str_t security_token;

	str_t key_scope;
	str_t signing_key;
	char signing_key_buf[EVP_MAX_MD_SIZE];
} curl_ext_s3_conf_t;

typedef struct {
	struct curl_slist* headers;
	char* url;
} curl_ext_s3_ctx_t;


static str_t curl_ext_s3_aws4_request = str_init("aws4_request");

static str_t curl_ext_s3_aws4 = str_init("AWS4");

static str_t curl_ext_s3_service = str_init("s3");


static char*
curl_ext_s3_hex_dump(char* dst, unsigned char* src, size_t len)
{
	static char hex[] = "0123456789abcdef";

	while (len--)
	{
		*dst++ = hex[*src >> 4];
		*dst++ = hex[*src++ & 0xf];
	}

	return dst;
}

static void
curl_ext_s3_sha256_hex_buf(str_t* message, char* digest)
{
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256_CTX sha256;

	SHA256_Init(&sha256);
	SHA256_Update(&sha256, message->data, message->len);
	SHA256_Final(hash, &sha256);

	curl_ext_s3_hex_dump(digest, hash, sizeof(hash));
}

static bool_t
curl_ext_s3_hmac_sha256(str_t* key, str_t* message, str_t* dest)
{
	unsigned hash_len;
	HMAC_CTX* hmac;

#if (OPENSSL_VERSION_NUMBER < 0x10100000L)
	HMAC_CTX hmac_buf;

	hmac = &hmac_buf;
	HMAC_CTX_init(hmac);
#else
	hmac = HMAC_CTX_new();
	if (hmac == NULL)
	{
		error(0, "HMAC_CTX_new failed");
		return FALSE;
	}
#endif

	HMAC_Init_ex(hmac, key->data, key->len, EVP_sha256(), NULL);
	HMAC_Update(hmac, (unsigned char*)message->data, message->len);
	HMAC_Final(hmac, (unsigned char*)dest->data, &hash_len);

#if (OPENSSL_VERSION_NUMBER < 0x10100000L)
	HMAC_CTX_cleanup(hmac);
#else
	HMAC_CTX_free(hmac);
#endif

	dest->len = hash_len;

	return TRUE;
}

static bool_t
curl_ext_s3_hmac_sha256_hex(str_t* key, str_t* message, str_t* dest)
{
	unsigned char hash_buf[EVP_MAX_MD_SIZE];
	str_t hash;

	hash.data = (char*)hash_buf;

	if (!curl_ext_s3_hmac_sha256(key, message, &hash))
	{
		return FALSE;
	}

	dest->len = curl_ext_s3_hex_dump(dest->data, hash_buf, hash.len) -
		dest->data;
	return TRUE;
}

static void*
curl_ext_s3_conf_create()
{
	curl_ext_s3_conf_t* conf;

	conf = calloc(sizeof(*conf), 1);
	if (conf == NULL)
	{
		error(0, "alloc conf failed");
		return NULL;
	}

	return conf;
}

static int
curl_ext_s3_conf_handler(void* c, const char* name, const char* value)
{
	curl_ext_s3_conf_t* conf = c;
	str_t* dest;

	if (strcasecmp(name, "region") == 0)
	{
		dest = &conf->region;
	}
	else if (strcasecmp(name, "access_key") == 0)
	{
		dest = &conf->access_key;
	}
	else if (strcasecmp(name, "secret_key") == 0)
	{
		dest = &conf->secret_key;
	}
	else if (strcasecmp(name, "security_token") == 0)
	{
		dest = &conf->security_token;
	}
	else
	{
		return 1;
	}

	dest->len = strlen(value);
	dest->data = malloc(dest->len + 1);
	if (dest->data == NULL)
	{
		error(0, "alloc conf param failed");
		return 0;
	}

	memcpy(dest->data, value, dest->len + 1);

	return 1;
}

static bool_t
curl_ext_s3_conf_init(void* c)
{
	curl_ext_s3_conf_t* conf = c;
	struct tm* tm;
	time_t t;
	str_t* key_scope;
	str_t* key;
	str_t secret_key_prefix;
	str_t date;
	char* p;
	char date_buf[CURL_EXT_S3_AMZ_DATE_LEN];

	if (!conf->region.len || !conf->access_key.len || !conf->secret_key.len)
	{
		return TRUE;
	}

	// get the date
	t = time(NULL);
	tm = gmtime(&t);
	if (tm == NULL)
	{
		error(0, "gmtime failed");
		return FALSE;
	}

	date.len = strftime((char*)date_buf, sizeof(date_buf),
		CURL_EXT_S3_AMZ_DATE_FORMAT, tm);
	if (date.len == 0)
	{
		error(0, "strftime failed");
		return FALSE;
	}
	date.data = date_buf;

	// init the key scope
	key_scope = &conf->key_scope;

	key_scope->data = malloc(date.len + conf->region.len +
		curl_ext_s3_service.len +
		curl_ext_s3_aws4_request.len + 4);
	if (key_scope->data == NULL)
	{
		error(0, "alloc key scope failed");
		return FALSE;
	}

	p = key_scope->data;
	p = mem_copy_str(p, date);
	*p++ = '/';
	p = mem_copy_str(p, conf->region);
	*p++ = '/';
	p = mem_copy_str(p, curl_ext_s3_service);
	*p++ = '/';
	p = mem_copy_str(p, curl_ext_s3_aws4_request);
	*p = '\0';
	key_scope->len = p - key_scope->data;

	key = &conf->signing_key;
	key->data = conf->signing_key_buf;

	// add prefix to secret key
	secret_key_prefix.data = malloc(curl_ext_s3_aws4.len +
		conf->secret_key.len);
	if (secret_key_prefix.data == NULL)
	{
		error(0, "alloc key prefix failed");
		return FALSE;
	}

	p = secret_key_prefix.data;
	p = mem_copy_str(p, curl_ext_s3_aws4);
	p = mem_copy_str(p, conf->secret_key);
	secret_key_prefix.len = p - secret_key_prefix.data;

	if (!curl_ext_s3_hmac_sha256(&secret_key_prefix, &date, key) ||
		!curl_ext_s3_hmac_sha256(key, &conf->region, key) ||
		!curl_ext_s3_hmac_sha256(key, &curl_ext_s3_service, key) ||
		!curl_ext_s3_hmac_sha256(key, &curl_ext_s3_aws4_request, key))
	{
		free(secret_key_prefix.data);
		return FALSE;
	}

	free(secret_key_prefix.data);
	return TRUE;
}

static void
curl_ext_s3_conf_free(void* c)
{
	curl_ext_s3_conf_t* conf = c;

	free(conf->key_scope.data);
	free(conf->region.data);
	free(conf->access_key.data);
	free(conf->secret_key.data);
	free(conf->security_token.data);
	free(conf);
}

static bool_t
curl_ext_s3_get_canonical_headers(curl_ext_s3_conf_t* conf,
	str_t* host, str_t* date, str_t* canonical_headers, str_t* signed_headers)
{
	static const char canonical_headers_template[] =
		"host:%.*s\n"
		"x-amz-content-sha256:" CURL_EXT_S3_EMPTY_SHA256 "\n"
		"x-amz-date:%.*s\n";

	char* p;
	size_t size;

	size = sizeof(canonical_headers_template) + host->len + date->len;
	if (conf->security_token.len > 0)
	{
		size += sizeof(CURL_EXT_S3_SECURITY_TOKEN_CANON) - 1 +
			conf->security_token.len + 1;
	}

	p = malloc(size);
	if (p == NULL)
	{
		error(0, "canonical headers alloc failed");
		return FALSE;
	}

	canonical_headers->data = p;

	p += sprintf(p, canonical_headers_template, str_f(*host), str_f(*date));

	if (conf->security_token.len > 0)
	{
		p = mem_copy(p, CURL_EXT_S3_SECURITY_TOKEN_CANON,
			sizeof(CURL_EXT_S3_SECURITY_TOKEN_CANON) - 1);
		p = mem_copy_str(p, conf->security_token);
		*p++ = '\n';

		str_set(signed_headers,
			"host;x-amz-content-sha256;x-amz-date;x-amz-security-token");
	}
	else
	{
		str_set(signed_headers, "host;x-amz-content-sha256;x-amz-date");
	}

	canonical_headers->len = p - canonical_headers->data;

	return TRUE;
}

static bool_t
curl_ext_s3_get_auth_header(curl_ext_s3_conf_t* conf,
	str_t* host, str_t* uri, str_t* date, str_t* result)
{
	static const char canonical_request_template[] =
		"GET\n"
		"%.*s\n"
		"\n"
		"%.*s\n"
		"%.*s\n"
		CURL_EXT_S3_EMPTY_SHA256;

	static const char string_to_sign_template[] =
		"AWS4-HMAC-SHA256\n"
		"%.*s\n"
		"%.*s\n"
		"%.*s";

	static const char authorization_header_template[] =
		"Authorization: AWS4-HMAC-SHA256 Credential=%.*s/%.*s, "
		"SignedHeaders=%.*s, "
		"Signature=%.*s";

	str_t canonical_headers;
	str_t canonical_request;
	str_t canonical_sha;
	str_t signed_headers;
	str_t string_to_sign;
	str_t signature;
	bool_t rc = FALSE;

	char canonical_sha_buf[CURL_EXT_S3_SHA256_HEX_LEN];
	char signature_buf[CURL_EXT_S3_HMAC_HEX_LEN];

	string_to_sign.data = NULL;
	canonical_request.data = NULL;
	canonical_headers.data = NULL;

	// canonical headers
	if (!curl_ext_s3_get_canonical_headers(conf, host, date,
		&canonical_headers, &signed_headers))
	{
		goto done;
	}

	// canonical request
	canonical_request.data = malloc(sizeof(canonical_request_template) +
		uri->len + canonical_headers.len + signed_headers.len);
	if (canonical_request.data == NULL)
	{
		error(0, "canonical request alloc failed");
		goto done;
	}

	canonical_request.len = sprintf(canonical_request.data,
		canonical_request_template, str_f(*uri), str_f(canonical_headers),
		str_f(signed_headers));

	curl_ext_s3_sha256_hex_buf(&canonical_request, canonical_sha_buf);

	canonical_sha.data = canonical_sha_buf;
	canonical_sha.len = sizeof(canonical_sha_buf);

	// string to sign
	string_to_sign.data = malloc(sizeof(string_to_sign_template) + date->len +
		conf->key_scope.len + canonical_sha.len);
	if (string_to_sign.data == NULL)
	{
		error(0, "string_to_sign alloc failed");
		goto done;
	}

	string_to_sign.len = sprintf(string_to_sign.data, string_to_sign_template,
		str_f(*date), str_f(conf->key_scope), str_f(canonical_sha));

	signature.data = signature_buf;

	if (!curl_ext_s3_hmac_sha256_hex(&conf->signing_key,
		&string_to_sign, &signature))
	{
		goto done;
	}

	// auth header
	result->data = malloc(sizeof(authorization_header_template) +
		conf->access_key.len + conf->key_scope.len + signed_headers.len +
		signature.len);
	if (result->data == NULL)
	{
		error(0, "authorization alloc failed");
		goto done;
	}

	result->len = sprintf(result->data, authorization_header_template,
		str_f(conf->access_key), str_f(conf->key_scope), str_f(signed_headers),
		str_f(signature));

	rc = TRUE;

done:

	free(string_to_sign.data);
	free(canonical_request.data);
	free(canonical_headers.data);

	return rc;
}

static intptr_t
curl_ext_s3_hextoi(char *line, size_t n)
{
	intptr_t value;
	char c, ch;

	if (n == 0) {
		return -1;
	}

	for (value = 0; n--; line++) {
		ch = *line;

		if (ch >= '0' && ch <= '9') {
			value = value * 16 + (ch - '0');
			continue;
		}

		c = (char) (ch | 0x20);

		if (c >= 'a' && c <= 'f') {
			value = value * 16 + (c - 'a' + 10);
			continue;
		}

		return -1;
	}

	return value;
}

static uintptr_t
curl_ext_s3_normalize_uri(char *dst, char *src, size_t size)
{
	static char hex[] = "0123456789ABCDEF";
	uintptr_t n;
	intptr_t num;
	char ch;

					/* " ", "#", "%", "?", %00-%1F, %7F-%FF */

					/* not ALPHA, DIGIT, "-", ".", "_", "~" */

	static uint32_t escape[] = {
		0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */

					/* ?>=< ;:98 7654 3210  /.-, +*)( '&%$ #"!  */
		0xfc001fff, /* 1111 1100 0000 0000  0001 1111 1111 1111 */

					/* _^]\ [ZYX WVUT SRQP  ONML KJIH GFED CBA@ */
		0x78000001, /* 0111 1000 0000 0000  0000 0000 0000 0001 */

					/*  ~}| {zyx wvut srqp  onml kjih gfed cba` */
		0xb8000001, /* 1011 1000 0000 0000  0000 0000 0000 0001 */

		0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
		0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
		0xffffffff, /* 1111 1111 1111 1111  1111 1111 1111 1111 */
		0xffffffff  /* 1111 1111 1111 1111  1111 1111 1111 1111 */
	};


	if (dst == NULL) {

		/* find the number of the characters to be escaped */

		n = 0;

		while (size) {
			if (escape[*src >> 5] & (1U << (*src & 0x1f))) {
				n++;
			}

			src++;
			size--;
		}

		return (uintptr_t) n;
	}

	while (size) {

		switch (*src) {

		case '+':
			ch = ' ';
			src++;
			size--;
			break;

		case '%':
			if (size >= 3) {
				num = curl_ext_s3_hextoi(src + 1, 2);
				if (num >= 0) {
					ch = num;
					src += 3;
					size -= 3;
					break;
				}
			}

			/* fall through */

		default:
			ch = *src++;
			size--;
		}

		if (escape[ch >> 5] & (1U << (ch & 0x1f))) {
			*dst++ = '%';
			*dst++ = hex[ch >> 4];
			*dst++ = hex[ch & 0xf];

		} else {
			*dst++ = ch;
		}
	}

	return (uintptr_t) dst;
}

static void
curl_ext_s3_free(void* data)
{
	curl_ext_s3_ctx_t* ctx = data;

	free(ctx->url);
	curl_slist_free_all(ctx->headers);
	free(ctx);
}

static void*
curl_ext_s3_init(void* c, str_t* url, CURL* curl)
{
	curl_ext_s3_conf_t* conf = c;
	curl_ext_s3_ctx_t* ctx;
	struct curl_slist* headers;
	struct tm* tm;
	uintptr_t escape;
	CURLcode res;
	str_t norm_uri;
	str_t bucket;
	str_t date;
	str_t host;
	str_t auth;
	str_t uri;
	char *header;
	char *p;
	time_t t;

	char date_buf[CURL_EXT_S3_AMZ_TIME_LEN];
	char amz_date[COMPESSED_FILE_S3_AMZ_DATE_HEADER_LEN];

	ctx = NULL;
	host.data = NULL;
	norm_uri.data = NULL;

	if (!conf->key_scope.len)
	{
		error(0, "missing s3 required params");
		goto failed;
	}

	// split the uri and bucket
	uri.data = memchr(url->data, '/', url->len);
	if (uri.data == NULL)
	{
		error(0, "failed to parse s3 url");
		goto failed;
	}
	uri.len = url->data + url->len - uri.data;

	bucket.data = url->data;
	bucket.len = uri.data - bucket.data;

	// normalize the uri
	escape = curl_ext_s3_normalize_uri(NULL, uri.data, uri.len);

	norm_uri.data = malloc(uri.len + 2 * escape);
	if (norm_uri.data == NULL)
	{
		error(0, "failed to alloc uri");
		goto failed;
	}

	norm_uri.len = (char *) curl_ext_s3_normalize_uri(norm_uri.data, uri.data,
		uri.len) - norm_uri.data;

	// get the host
	host.data = malloc(sizeof(COMPESSED_FILE_S3_HOST) + bucket.len +
		conf->region.len);
	if (host.data == NULL)
	{
		error(0, "failed to alloc host");
		goto failed;
	}

	host.len = sprintf(host.data, "%.*s.s3.%.*s.amazonaws.com", str_f(bucket),
		str_f(conf->region));

	// get the date
	t = time(NULL);
	tm = gmtime(&t);
	if (tm == NULL)
	{
		error(0, "gmtime failed");
		goto failed;
	}

	date.len = strftime(date_buf, sizeof(date_buf),
		CURL_EXT_S3_AMZ_TIME_FORMAT, tm);
	if (date.len == 0)
	{
		error(0, "strftime failed");
		goto failed;
	}
	date.data = date_buf;

	// alloc ctx
	ctx = calloc(sizeof(*ctx), 1);
	if (ctx == NULL)
	{
		error(0, "failed to alloc s3 ctx");
		goto failed;
	}

	// authorization header
	if (!curl_ext_s3_get_auth_header(conf, &host, &norm_uri, &date, &auth))
	{
		goto failed;
	}

	headers = curl_slist_append(ctx->headers, auth.data);

	free(auth.data);

	if (headers == NULL)
	{
		error(0, "curl_slist_append failed (1)");
		goto failed;
	}
	ctx->headers = headers;

	// amz date header
	sprintf(amz_date, COMPESSED_FILE_S3_AMZ_DATE_HEADER, str_f(date));
	headers = curl_slist_append(ctx->headers, amz_date);
	if (headers == NULL)
	{
		error(0, "curl_slist_append failed (2)");
		goto failed;
	}
	ctx->headers = headers;

	// content-sha256 header
	headers = curl_slist_append(ctx->headers, "X-Amz-Content-SHA256: "
		CURL_EXT_S3_EMPTY_SHA256);
	if (headers == NULL)
	{
		error(0, "curl_slist_append failed (3)");
		goto failed;
	}
	ctx->headers = headers;

	// security-token header
	if (conf->security_token.len > 0)
	{
		header = malloc(sizeof(CURL_EXT_S3_SECURITY_TOKEN) +
			conf->security_token.len);
		if (header == NULL)
		{
			error(0, "failed to alloc security token");
			goto failed;
		}

		p = mem_copy(header, CURL_EXT_S3_SECURITY_TOKEN,
			sizeof(CURL_EXT_S3_SECURITY_TOKEN) - 1);
		p = mem_copy_str(p, conf->security_token);
		*p = '\0';

		headers = curl_slist_append(ctx->headers, header);

		free(header);

		if (headers == NULL)
		{
			error(0, "curl_slist_append failed (4)");
			goto failed;
		}
		ctx->headers = headers;
	}

	// set headers
	res = curl_easy_setopt(curl, CURLOPT_HTTPHEADER, ctx->headers);
	if (res != CURLE_OK)
	{
		error(0, "curl_easy_setopt(CURLOPT_HTTPHEADER) failed %d", res);
		goto failed;
	}

	// set the url
	ctx->url = malloc(sizeof(COMPESSED_FILE_S3_SCHEME) + host.len +
		norm_uri.len);
	if (ctx->url == NULL)
	{
		error(0, "alloc url failed");
		goto failed;
	}

	sprintf(ctx->url, "%s%.*s%.*s", COMPESSED_FILE_S3_SCHEME, str_f(host),
		str_f(norm_uri));

	res = curl_easy_setopt(curl, CURLOPT_URL, ctx->url);
	if (res != CURLE_OK)
	{
		error(0, "curl_easy_setopt(CURLOPT_URL) failed %d", res);
		goto failed;
	}

	free(host.data);
	free(norm_uri.data);
	return ctx;

failed:

	if (ctx != NULL)
	{
		curl_ext_s3_free(ctx);
	}

	free(host.data);
	free(norm_uri.data);
	return NULL;
}

curl_ext_module_t curl_ext_s3 = {
	"s3",
	curl_ext_s3_conf_create,
	curl_ext_s3_conf_init,
	curl_ext_s3_conf_free,
	curl_ext_s3_conf_handler,

	str_init("s3://"),
	curl_ext_s3_init,
	curl_ext_s3_free,
};
