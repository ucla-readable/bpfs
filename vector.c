/* This file is part of Featherstitch. Featherstitch is copyright 2005-2008 The
 * Regents of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include <lib/platform.h>
#include <lib/vector.h>


static void ** vector_create_elts(size_t n);
static void    vector_destroy_elts(vector_t * v);
static int     vector_grow(vector_t * v);

# define INIT_CAPACITY 10


//
// Construction/destruction

vector_t * vector_create(void)
{
	// Create a vector with no elements, but with a capacity.

	vector_t * v = vector_create_size(INIT_CAPACITY);
	if (!v)
		return NULL;

	v->size = 0;
	return v;
}

vector_t * vector_create_size(size_t n)
{
	vector_t * v = malloc(sizeof(*v));
	if (!v)
		return NULL;

	v->size = n;
	v->elts = vector_create_elts(n);
	if (!v->elts)
	{
		free(v);
		return NULL;
	}
	memset(v->elts, 0, n*sizeof(v->elts));
	v->capacity = n;

	return v;
}

void vector_destroy(vector_t * v)
{
	vector_destroy_elts(v);
	free(v);
}

static void ** vector_create_elts(size_t n)
{
	void ** elts = smalloc(n*sizeof(*elts));
	return elts;
}

static void vector_destroy_elts(vector_t * v)
{
	sfree(v->elts, v->capacity*sizeof(*v->elts));
	v->elts = NULL;
	v->size = 0;
	v->capacity = 0;
}


//
// General

// vector_size() inlined

// vector_empty() inlined

int vector_push_back(vector_t * v, void * elt)
{
	int r;
	if (v->size == v->capacity)
	{
		if ((r = vector_grow(v)) < 0)
			return r;
	}

	v->elts[v->size++] = elt;
	return 0;
}

int vector_push_back_vector(vector_t * v, const vector_t * v2)
{
	size_t v2_size = vector_size(v2);
	size_t i;
	int r;

	r = vector_reserve(v, vector_size(v) + v2_size);
	if (r < 0)
		return r;

	for (i=0; i < v2_size; i++)
	{
		r = vector_push_back(v, vector_elt((vector_t *) v2, i));
		assert(r >= 0); // no error since space is pre-allocated
	}

	return 0;
}

// vector_pop_back() inlined

void vector_erase(vector_t * v, size_t i)
{
	for (; i+1 < v->size; i++)
		v->elts[i] = v->elts[i+1];
	v->size--;
}

void vector_clear(vector_t * v)
{
	v->size = 0;
}

#ifndef __KERNEL__
void vector_sort(vector_t *v, int (*compar)(const void *a, const void *b))
{
	if (v->size < 2) return;
	qsort(v->elts, v->size, sizeof(void*), compar);
}
#endif


//
// Element access

// vector_elt() inlined

// vector_elt_front() inlined

// vector_elt_end() inlined

bool vector_contains(vector_t * v, void * elt)
{
	size_t i;
	for (i = 0; i < v->size; i++)
		if(v->elts[i] == elt)
			return 1;
	return 0;
}

//
// Growing/shrinking

size_t vector_capacity(const vector_t * v)
{
	return v->capacity;
}

int vector_reserve(vector_t * v, size_t n)
{
	size_t i;
	const size_t n_elts = v->size;
	void ** elts;

	if (n <= v->capacity)
		return 1;

	elts = vector_create_elts(n);
	if (!elts)
		return -ENOMEM;

	for (i=0; i < n_elts; i++)
		elts[i] = v->elts[i];

	vector_destroy_elts(v);
	v->elts = elts;
	v->size = n_elts;
	v->capacity = n;

	return 0;
}

static int vector_grow(vector_t * v)
{
	return vector_reserve(v, 2*v->capacity);
}
