#include <stdio.h>
#include <string.h>
#include "common.h"

// globals
char* program_name;

void 
verror(int errnum, const char *message, va_list args)
{
	char const *s;
	
	fflush(stdout);

	fprintf(stderr, "%s: ", program_name);
	vfprintf(stderr, message, args);
	if (errnum)
	{
		s = strerror(errnum);
		fprintf(stderr, ": %s", s);
	}
	putc('\n', stderr);
	fflush(stderr);
}

void
error(int errnum, const char *message, ...)
{
	va_list args;

	va_start(args, message);
	verror(errnum, message, args);
	va_end(args);
}
