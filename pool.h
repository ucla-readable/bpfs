/* This file is part of Featherstitch. Featherstitch is copyright 2005-2007 The
 * Regents of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef FSTITCH_LIB_POOL_H
#define FSTITCH_LIB_POOL_H

#include <lib/platform.h>

// Set to 1 to use malloc() and free() instead of pools. Useful for debugging.
#define POOL_MALLOC 0

#if !POOL_MALLOC

#define POOLSIZE(type) ((int) ((PAGE_SIZE - sizeof(void*)) / sizeof(type)))

// Create a pool, allocator, and deallocators for 'type'.
// API: type* name_alloc(), name_free(type*), name_free_all().
#define DECLARE_POOL(name, type) \
	struct name##_pool { \
		struct name##_pool * next; \
		type elts[POOLSIZE(type)]; \
	}; \
	static type * name##_free_list; \
	static struct name##_pool * name##_free_pool; \
	\
	static type * alloc_##name##_pool(void) \
	{ \
		struct name##_pool * pool; \
		int i; \
		if(!(pool = malloc(sizeof(*pool)))) \
			return NULL; \
		pool->next = name##_free_pool; \
		name##_free_pool = pool; \
		for(i = 1; i < POOLSIZE(type); i++) \
			* ((type **) &pool->elts[i]) = &pool->elts[i-1]; \
		* ((type **) &pool->elts[0]) = name##_free_list; \
		name##_free_list = &pool->elts[POOLSIZE(type) - 1]; \
		return name##_free_list; \
	} \
	static __inline type * name##_alloc(void) __attribute__((always_inline)); \
	static __inline type * name##_alloc(void) \
	{ \
		type * p; \
		if(unlikely(!name##_free_list)) \
			if(unlikely(!alloc_##name##_pool())) \
				return NULL; \
		p = name##_free_list; \
		name##_free_list = * ((type **) p); \
		return p; \
	} \
	static __inline void name##_free(type * p) __attribute__((always_inline)); \
	static __inline void name##_free(type * p) \
	{ \
		* ((type **) p) = name##_free_list; \
		name##_free_list = p; \
	} \
	static void name##_free_all(void) \
	{ \
		struct name##_pool * pool; \
		while((pool = name##_free_pool)) \
		{ \
			name##_free_pool = pool->next; \
			free(pool); \
		} \
	}

#else

# define DECLARE_POOL(name, type) \
	static type * name##_alloc(void) { return malloc(sizeof(type)); } \
	static void name##_free(type * p) { free(p); } \
	static void name##_free_all(void) { }

#endif

#endif
