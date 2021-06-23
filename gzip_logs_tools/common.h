#ifndef __COMMON_H__
#define __COMMON_H__

// includes
#include <stdarg.h>
#include <inttypes.h>
#include <sys/types.h>

// constants
#ifndef TRUE
#define TRUE (1)
#endif

#ifndef FALSE
#define FALSE (0)
#endif

// macros
#define array_entries(x) (sizeof(x) / sizeof(x[0]))
#define min(x, y) (((x) < (y)) ? (x) : (y))

#define mem_copy(dst, src, n)	(((char *) memcpy(dst, src, n)) + (n))
#define mem_copy_str(dst, src)	mem_copy(dst, (src).data, (src).len)
#define str_init(str)			{ sizeof(str) - 1, (char *) str }
#define str_f(str)				(int)(str).len, (str).data
#define str_set(str, text)                                               \
    (str)->len = sizeof(text) - 1; (str)->data = (char *) text

// typedefs
typedef intptr_t bool_t;

typedef struct {
	size_t len;
	char* data;
} str_t;

// globals
extern char* program_name;

// functions
void verror(int errnum, const char *message, va_list args);
void error(int errnum, const char *message, ...);

#endif // __COMMON_H__
