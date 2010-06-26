/* This file is part of Featherstitch. Featherstitch is copyright 2005-2007 The
 * Regents of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef FSTITCH_INC_HASH_MAP_H
#define FSTITCH_INC_HASH_MAP_H

/* Set to check for illegal hash map modifications during iteration.
 * If hash map iteration code tries to deref bad pointers, try this. */
/* values: 0 (normal), 1 (debug) */
#ifndef NDEBUG
# define HASH_MAP_IT_MOD_DEBUG 1
#else
# define HASH_MAP_IT_MOD_DEBUG 0
#endif

/* Set to track the number of elements in hash map buckets. */
/* values: 0 (normal), 1 (track) */
#define HASH_MAP_TRACK_BUCKET_SIZES 0

typedef struct hash_map_elt hash_map_elt_t;
typedef struct chain_elt chain_elt_t;
typedef struct hash_map hash_map_t;

struct hash_map_elt {
	void * key;
	void * val;
};

struct chain_elt;

struct hash_map;

int hash_map_init(void);

// Create a hash_map.
hash_map_t * hash_map_create(void);
hash_map_t * hash_map_create_ptr(void);
hash_map_t * hash_map_create_str(void);
// Create a hash_map, reserve space for n entries, allow/don't auto resizing.
hash_map_t * hash_map_create_size(size_t n, bool auto_resize);
hash_map_t * hash_map_create_size_ptr(size_t n, bool auto_resize);
hash_map_t * hash_map_create_size_str(size_t n, bool auto_resize);
// Create a hash map that contains the same elements as hm
hash_map_t * hash_map_copy(const hash_map_t * hm);
// Destroy a hash_map, does not destroy keys or vals.
void         hash_map_destroy(hash_map_t * hm);

// Return number of items in the hash_map.
size_t hash_map_size(const hash_map_t * hm);
// Return whether hash_map is empty.
bool   hash_map_empty(const hash_map_t * hm);
// Insert the given key-val pair, updating k's v if k exists.
// Returns 0 or 1 on success, or -ENOMEM.
int    hash_map_insert(hash_map_t * hm, void * k, void * v);
// Remove the given key-val pair, does not destory key or val.
// Returns k's value on success, NULL if k is not in the hash_map.
void * hash_map_erase(hash_map_t * hm, const void * k);
// Change the mapping from oldk->val to be newk->val.
// Returns 0 on success, -EEXIST if newk exists, or -ENOENT if oldk does not exist.
int    hash_map_change_key(hash_map_t * hm, void * oldk, void * newk);
// Remove all key-val pairs, does not destroy keys or vals.
void   hash_map_clear(hash_map_t * hm);
// Return the val associated with k.
void * hash_map_find_val(const hash_map_t * hm, const void * k);
// Return the key and val associated with k.
hash_map_elt_t hash_map_find_elt(const hash_map_t * hm, const void * k);
// Return a pointer to the internal key and val associated with k.
// Useful to expose the address of the internal hash_map_elt_t->val.
// The value of key must not be changed through this pointer.
// The returned pointer will become invalid upon erasure of this element.
hash_map_elt_t * hash_map_find_eltp(const hash_map_t * hm, const void * k);

// Return the number of buckets currently allocated.
size_t hash_map_bucket_count(const hash_map_t * hm);
// Resize the number of buckets to n.
// Returns 0 on success, 1 on no resize needed, or -ENOMEM.
int    hash_map_resize(hash_map_t * hm, size_t n);

#if HASH_MAP_TRACK_BUCKET_SIZES
struct vector;
// Return a vector of each bucket's maximum size (vector elts are size_ts)
const struct vector * hash_map_max_sizes(const hash_map_t * hm);
#endif

// Iteration (current)

struct hash_map_it2 {
	void * key; // key of the current map entry
	void * val; // value of the current map entry
	struct {
		hash_map_t * hm;
		size_t next_bucket;
		chain_elt_t * next_elt;
#if HASH_MAP_IT_MOD_DEBUG
		size_t loose_version;
#endif
	} internal;
};
typedef struct hash_map_it2 hash_map_it2_t;

hash_map_it2_t hash_map_it2_create(hash_map_t * hm);
// Iterate through the hash map values using it.
// - Returns false once the end of the hash map is reached.
// - Behavior is undefined if you begin iterating, then insert an element,
//   resize the map, or delete the next element, and then continue iterating
//   using the old iterator. (Define HASH_MAP_IT_MOD_DEBUG to detect some
//   cases.)
bool hash_map_it2_next(hash_map_it2_t * it);


// Iteration (deprecated)

struct hash_map_it {
	hash_map_t * hm;
	size_t bucket;
	chain_elt_t * elt;
#if HASH_MAP_IT_MOD_DEBUG
	size_t version;
#endif
};
typedef struct hash_map_it hash_map_it_t;

void hash_map_it_init(hash_map_it_t * it, hash_map_t * hm);
// Iterate through the hash map values using hm_it.
// - Returns NULL when the end of the hash map is reached.
// - Behavior is undefined if you begin iterating, modify hm, and then continue
//   iterating using the old hm_it. (Define HASH_MAP_IT_MOD_DEBUG to detect.)
void * hash_map_val_next(hash_map_it_t * it);
// Iterate through the hash map values using hm_it.
// - key is NULL when the end of the hash map is reached.
// - Behavior is undefined if you begin iterating, modify hm, and then continue
//   iterating using the old hm_it. (Define HASH_MAP_IT_MOD_DEBUG to detect.)
hash_map_elt_t hash_map_elt_next(hash_map_it_t * it);

#endif /* !FSTITCH_INC_HASH_MAP_H */
