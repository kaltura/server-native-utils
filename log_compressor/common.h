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
#define min(x, y) (((x) < (y)) ? (x) : (y))

// typedefs
typedef intptr_t bool_t;

// globals
extern char* program_name;

// functions
void verror(int errnum, const char *message, va_list args);
void error(int errnum, const char *message, ...);

#endif // __COMMON_H__
