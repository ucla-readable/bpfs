/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef UTIL_H
#define UTIL_H

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

// if cond is false, display message and then exit
#define xassert(cond) \
	do { \
		if (!(cond))  \
		{ \
			fprintf(stderr, "Not true, but should be: %s\n", # cond); \
			assert(0); \
			exit(1); \
		} \
	} while (0)

// if syscall exp call fails, display message and errno and then exit
#define xsyscall(call, format...) \
	({ \
		int err = call; \
		if (err < 0)	\
		{ \
			int err = errno; \
			fprintf(stderr, "%s: %s\n", # call, strerror(err));	\
			assert(0); \
			exit(1); \
		} \
		err; \
	})

// if function call exp call returns < 0, display message and value and
// then exit
#define xcall(call, format...) \
	({ \
		int err = call; \
		if (err < 0)	\
		{ \
			fprintf(stderr, "%s: %s\n", # call, strerror(-err)); \
			assert(0); \
			exit(1); \
		} \
		err; \
	})

#define UNUSED(x) do { (void) x; } while(0)

// static_assert(x) will generate a compile-time error if 'x' is false.
#define static_assert(x) switch (x) case 0: case (x):

// Efficient min and max operations
#define MIN(_a, _b) \
	({ \
		typeof(_a) __a = (_a);  \
		typeof(_b) __b = (_b);  \
		__a <= __b ? __a : __b; \
	})
#define MAX(_a, _b) \
	({ \
		typeof(_a) __a = (_a);  \
		typeof(_b) __b = (_b);  \
		__a >= __b ? __a : __b; \
	})
#define MAXU64(_a, _b) \
	({ \
		uint64_t __a = (_a);  \
		uint64_t __b = (_b);  \
		__a >= __b ? __a : __b; \
	})

// Max operation that propagates constant expressions as constants
#define CMAX(_a, _b) ((_a) >= (_b) ? (_a) : (_b))

// 64-bit integer rounding; only works for n = power of two
// NOTE: ROUNDUP64() may eval n twice. This macro does not create a variable
// on the stack to avoid this because it prevents gcc from being able
// to evaluate the resulting expression at compile time.
#define ROUNDUP64(a, n)   (((uint64_t) (a) + n - 1) & ~(n - 1))
#define ROUNDDOWN64(a, n) (((uint64_t) (a)) & ~((n) - 1))

#define container_of(ptr, type, member) \
	({ \
		typeof(((type *) 0)->member) * __mptr = (ptr); \
		(type *) (uintptr_t) ((const char *) __mptr - offsetof(type, member)); \
	})

// Reinterpret a uint64_t as a void* and perform an equality check if
// these types are of different sizes
static __inline
void* u64_ptr(uint64_t x) __attribute__((always_inline));

static __inline
void* u64_ptr(uint64_t u)
{
	void *p = (void*) u;
	if (sizeof(p) < sizeof(u))
		xassert(((uint64_t) p) == u);
	return p;
}

#define NBLOCKS_FOR_NBYTES(nbytes) \
	(((nbytes) + BPFS_BLOCK_SIZE - 1) / BPFS_BLOCK_SIZE)

#define BPFS_TIME_NOW() \
	({ struct bpfs_time btime = {time(NULL)}; btime; })

typedef uint64_t bitmap_scan_t;

#endif
