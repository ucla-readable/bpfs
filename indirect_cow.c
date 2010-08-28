/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "indirect_cow.h"
#include "bpfs.h"
#include "hash_map.h"

#include <assert.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

#if INDIRECT_COW

#define DEBUG 0
#if DEBUG
# define Dprintf(x...) fprintf(stderr, x)
#else
# define Dprintf(x...) do {} while(0)
#endif


struct block {
	uint64_t orig_blkno; // the block number of the block this block replaces
	uint64_t cow_blkno; // this block's block number
	char *dram; // new contents (in DRAM)
	bool required; // whether this block must be commited to DRAM
	struct block *parent; // block's parent. for integrity assertions.
	struct block *children_all; // all children of this block
	struct block *child_all_next; // this block's entry in the child all list
	struct block *children_cow; // children of this block that are cowed
	struct block *child_cow_next; // this block's entry in the child cow list
};

// super + max inode tree height + max file tree height (is +2 correct?):
#define PARENT_STACK_SIZE (2 * BPFS_TREE_MAX_HEIGHT + 2)

struct parent_stack {
	struct block *stack[PARENT_STACK_SIZE];
	unsigned height;
};


static bool indirect_cow_inited;
static hash_map_t *blkno_map_orig; // orig block number -> struct block*
static hash_map_t *blkno_map_cow;  //  cow block number -> struct block*
static struct parent_stack parent_stack;


static struct block* parent_get(void)
{
	if (!parent_stack.height)
		return NULL;
	return parent_stack.stack[parent_stack.height - 1];
}

static struct block* block_get_either(uint64_t blkno)
{
	void *key = u64_ptr(blkno);
	struct block *block = hash_map_find_val(blkno_map_cow, key);
	if (block)
		return block;
	block = hash_map_find_val(blkno_map_orig, key);
	assert(!block || block->cow_blkno == BPFS_BLOCKNO_INVALID);
	return block;
}

static struct block* block_create(uint64_t orig_blkno, uint64_t cow_blkno)
{
	struct block *parent = parent_get();
	struct block *block = malloc(sizeof(*block));
	assert(!block_get_either(orig_blkno) && !block_get_either(cow_blkno));
	if (!block)
		return NULL;
	block->orig_blkno = orig_blkno;
	block->cow_blkno = cow_blkno;
	block->dram = NULL;
	block->required = false;
	block->parent = parent;
	block->children_all = NULL;
	block->children_cow = NULL;
	if (!parent)
	{
		assert(orig_blkno == BPFS_BLOCKNO_SUPER);
		block->child_all_next = NULL;
		block->child_cow_next = NULL;
	}
	else
	{
		assert(orig_blkno != BPFS_BLOCKNO_SUPER);
		assert(parent->orig_blkno != BPFS_BLOCKNO_SUPER
		       || !parent->children_all);
		block->child_all_next = parent->children_all;
		parent->children_all = block;
		if (cow_blkno == BPFS_BLOCKNO_INVALID)
			block->child_cow_next = NULL;
		else
		{
			block->child_cow_next = parent->children_cow;
			parent->children_cow = block;
		}
	}
	return block;
}


int indirect_cow_init(void)
{
	assert(!indirect_cow_inited);
	blkno_map_orig = hash_map_create_ptr();
	if (!blkno_map_orig)
		return -ENOMEM;
	blkno_map_cow = hash_map_create_ptr();
	if (!blkno_map_cow)
	{
		hash_map_destroy(blkno_map_orig);
		return -ENOMEM;
	}
	parent_stack.height = 0;
	indirect_cow_inited = true;
	return 0;
}

void indirect_cow_destroy(void)
{
	struct block *super;
	assert(indirect_cow_inited);
	assert(hash_map_size(blkno_map_orig) == 1);
	assert(hash_map_size(blkno_map_cow) == 1);

	indirect_cow_inited = false;

	super = hash_map_find_val(blkno_map_orig, u64_ptr(BPFS_BLOCKNO_SUPER));
	assert(super);
	(void) hash_map_erase(blkno_map_orig, u64_ptr(BPFS_BLOCKNO_SUPER));
	(void) hash_map_erase(blkno_map_cow, u64_ptr(super->cow_blkno));
	free(super->dram);
	free(super);

	hash_map_destroy(blkno_map_orig);
	hash_map_destroy(blkno_map_cow);
}


int indirect_cow_parent_push(uint64_t blkno)
{
	struct block *block = block_get_either(blkno);
	Dprintf("%s(blkno = %" PRIu64 ")\n", __FUNCTION__, blkno);

	if (!block)
	{
		int r;
		block = block_create(blkno, BPFS_BLOCKNO_INVALID);
		if (!block)
			return -ENOMEM;
		r = hash_map_insert(blkno_map_orig, u64_ptr(blkno), block);
		xcall(r); // TODO: destroy block
		assert(!r);
	}
	else
	{
#ifndef NDEBUG
		struct block *parent = parent_get();
		if (!parent)
		{
			assert(block->orig_blkno == BPFS_BLOCKNO_SUPER);
			assert(!block->child_all_next);
		}
		else if (block->orig_blkno == BPFS_BLOCKNO_SUPER)
		{
			assert(!block->child_all_next);
		}
		else
		{
			struct block *sibling = parent->children_all;
			for (; sibling && block != sibling;
			     sibling = sibling->child_all_next) ;
			assert(sibling);
			assert(block->parent == parent);
		}
#endif
	}

	parent_stack.height++;
	xassert(parent_stack.height <= PARENT_STACK_SIZE);
	parent_stack.stack[parent_stack.height - 1] = block;
	return 0;
}

void indirect_cow_parent_pop(uint64_t blkno)
{
	struct block *block;
	Dprintf("%s(blkno = %" PRIu64 ")\n", __FUNCTION__, blkno);
	assert(parent_stack.height);
	block = parent_stack.stack[parent_stack.height - 1];
	assert(blkno == block->orig_blkno || blkno == block->cow_blkno);
	parent_stack.height--;
}


int indirect_cow_block_cow(uint64_t orig_blkno, uint64_t cow_blkno)
{
	struct block *parent = parent_get();
	struct block *block = hash_map_find_val(blkno_map_orig,
	                                        u64_ptr(orig_blkno));
	bool new_block = !block;
	void *dram_void;
	int r;
	Dprintf("%s(orig_blkno = %" PRIu64 ", cow_blkno = %" PRIu64 ")\n",
	        __FUNCTION__, orig_blkno, cow_blkno);

	assert(orig_blkno != BPFS_BLOCKNO_INVALID);
	assert(cow_blkno != BPFS_BLOCKNO_INVALID);
	assert(cow_blkno != BPFS_BLOCKNO_SUPER);
	assert(orig_blkno != BPFS_BLOCKNO_SUPER_2);
	assert(cow_blkno != BPFS_BLOCKNO_SUPER_2);

	assert(!parent || parent->orig_blkno != BPFS_BLOCKNO_SUPER
	       || !parent->children_cow);

	if (new_block)
	{
		block = block_create(orig_blkno, cow_blkno);
		if (!block)
		{
			r = -ENOMEM;
			goto abort;
		}
	}
	else
	{
		assert(block->cow_blkno == BPFS_BLOCKNO_INVALID);
		assert(!block->child_cow_next);
		block->cow_blkno = cow_blkno;
		assert(block->parent == parent);
		if (parent)
		{
			assert(block->orig_blkno != BPFS_BLOCKNO_SUPER);
			block->child_cow_next = parent->children_cow;
			parent->children_cow = block;
		}
	}

	r = posix_memalign(&dram_void, BPFS_BLOCK_SIZE, BPFS_BLOCK_SIZE);
	if (r)
	{
		assert(r == ENOMEM);
		r = -r;
		goto abort;
	}
	block->dram = dram_void;

	if (new_block)
	{
		r = hash_map_insert(blkno_map_orig, u64_ptr(orig_blkno), block);
		if (r < 0)
			goto abort;
		assert(!r);
	}

	r = hash_map_insert(blkno_map_cow, u64_ptr(cow_blkno), block);
	if (r < 0)
		goto abort;
	assert(!r);

	return 0;

  abort:
	free(block->dram);
	block->cow_blkno = BPFS_BLOCKNO_INVALID;
	(void) hash_map_erase(blkno_map_cow, u64_ptr(cow_blkno));
	if (new_block)
	{
		(void) hash_map_erase(blkno_map_orig, u64_ptr(orig_blkno));
		free(block);
	}
	return r;
}

char* indirect_cow_block_get(uint64_t blkno)
{
	struct block *block = hash_map_find_val(blkno_map_cow, u64_ptr(blkno));
	if (!block)
		return NULL;
	return block->dram;
}

void indirect_cow_block_required(uint64_t blkno)
{
	struct block *block = hash_map_find_val(blkno_map_cow, u64_ptr(blkno));
	Dprintf("%s(blkno = %" PRIu64 ")\n", __FUNCTION__, blkno);
	if (block)
		block->required = true;
	else
		assert(block_freshly_alloced(blkno));
}

void indirect_cow_block_direct(uint64_t blkno, unsigned off, unsigned size)
{
	struct block *block = hash_map_find_val(blkno_map_cow, u64_ptr(blkno));
	Dprintf("%s(blkno = %" PRIu64 ", off = %u, size = %u)\n",
	        __FUNCTION__, blkno, off, size);

	assert(blkno != BPFS_BLOCKNO_INVALID);
	assert(off < BPFS_BLOCK_SIZE && size <= BPFS_BLOCK_SIZE);
	assert(off + size <= BPFS_BLOCK_SIZE);

	if (!block
	    || block->orig_blkno == BPFS_BLOCKNO_INVALID
	    || block->cow_blkno == BPFS_BLOCKNO_INVALID)
		return;

	memcpy(get_block(block->orig_blkno) + off, block->dram + off, size);
}


static bool cow_is_atomically_writable(const struct block *block,
                                       uint64_t *atomic_new,
                                       unsigned *atomic_off)
{
	char *block_0 = get_block(block->orig_blkno);
	char *block_1 = block->dram;
	bool diff = false;
	unsigned off;

	assert(!!atomic_new == !!atomic_off);
	assert(block_0);
	assert(block_1);

	// BPFS_BLOCK_SIZE will indicate no difference
	if (atomic_off)
		*atomic_off = BPFS_BLOCK_SIZE;

	static_assert(ATOMIC_SIZE == 8);
	for (off = 0 ; off < BPFS_BLOCK_SIZE; off += ATOMIC_SIZE)
	{
		if (*(uint64_t*) (block_0 + off) != *(uint64_t*) (block_1 + off))
		{
			if (diff)
				return false;

			diff = true;
			if (atomic_new)
			{
				*atomic_new = *(uint64_t*) (block_1 + off);
				*atomic_off = off;
			}
		}
	}

	return true;
}

void indirect_cow_commit(void)
{
	struct block *super_block;
	struct block *notatomic_block;
	struct block *block;
	uint64_t atomic_blkno;
	uint64_t atomic_new;
	unsigned atomic_off;
	hash_map_it2_t it;
	char *block_bpram;

	Dprintf("%s()\n", __FUNCTION__);

	assert(!parent_stack.height);

	// Should contain at least the super block:
	assert(!hash_map_empty(blkno_map_cow));

	if (hash_map_size(blkno_map_cow) == 1)
	{
		block = hash_map_find_val(blkno_map_orig, u64_ptr(BPFS_BLOCKNO_SUPER));
		assert(block && block->cow_blkno != BPFS_BLOCKNO_INVALID);
		set_super(get_bpram_super());
		(void) hash_map_erase(blkno_map_cow, u64_ptr(block->cow_blkno));
		unfree_block(BPFS_BLOCKNO_SUPER);
		unalloc_block(block->cow_blkno);
		free(block->dram);
		block->dram = NULL;
		block->cow_blkno = BPFS_BLOCKNO_INVALID;

		it = hash_map_it2_create(blkno_map_orig);
		while (hash_map_it2_next(&it))
		{
			block = it.val;
			assert(!block->dram && block->cow_blkno == BPFS_BLOCKNO_INVALID);
			free(block);
		}
		hash_map_clear(blkno_map_orig);
		return;
	}

	super_block = hash_map_find_val(blkno_map_orig,
	                                u64_ptr(BPFS_BLOCKNO_SUPER));
	assert(super_block);
	assert(!super_block->required);
	assert(super_block->children_cow);
	assert(cow_is_atomically_writable(super_block, NULL, NULL));

	// Find the highest block that is atomically writable
	block = super_block;
	while (1)
	{
		struct block *child = block->children_cow;
		assert(child || block->required);
		if (child)
		{
			assert(!child->child_cow_next);
			if (!cow_is_atomically_writable(child, NULL, NULL))
				break;
		}
		if (block->required)
			break;
		block = child;
	}
	atomic_blkno = block->orig_blkno;
	xassert(cow_is_atomically_writable(block, &atomic_new, &atomic_off));
	notatomic_block = block->children_cow;

	// Revert the parents of notatomic_block to their original state
	block = super_block;
	assert(block != notatomic_block);
	set_super(get_bpram_super());
	while (block != notatomic_block)
	{
		struct block *cur = block;

		unalloc_block(block->cow_blkno);
		unfree_block(block->orig_blkno);

		(void) hash_map_erase(blkno_map_orig, u64_ptr(block->orig_blkno));
		(void) hash_map_erase(blkno_map_cow, u64_ptr(block->cow_blkno));
		free(block->dram);
		block = block->children_cow;
		free(cur);
	}

	// Copy CoW blocks to BPRAM
	it = hash_map_it2_create(blkno_map_cow);
	while (hash_map_it2_next(&it))
	{
		block = it.val;

		(void) hash_map_erase(blkno_map_orig, u64_ptr(block->orig_blkno));
		// Before the get_block() call so that get_block() gets bpram:
		(void) hash_map_erase(blkno_map_cow, u64_ptr(block->cow_blkno));

		block_bpram = get_block(block->cow_blkno);
		memcpy(block_bpram, block->dram, BPFS_BLOCK_SIZE);

		free(block->dram);
		free(block);
	}

	// Free the blocks that were not CoWed
	it = hash_map_it2_create(blkno_map_orig);
	while (hash_map_it2_next(&it))
	{
		block = it.val;

		assert(block->cow_blkno == BPFS_BLOCKNO_INVALID);
		assert(!block->dram);
		assert(!block->required);

		(void) hash_map_erase(blkno_map_orig, u64_ptr(block->orig_blkno));
		free(block);
	}

	// Atomically commit
	// (There can be nothing to commit when all CoWs were unnecessary
	//  or direct.)
	if (atomic_off != BPFS_BLOCK_SIZE)
	{
		block_bpram = get_block(atomic_blkno);
		*(uint64_t*) (block_bpram + atomic_off) = atomic_new;
	}
}

void indirect_cow_abort(void)
{
	hash_map_it2_t it;

	Dprintf("%s()\n", __FUNCTION__);

	assert(!parent_stack.height);

	set_super(get_bpram_super());

	it = hash_map_it2_create(blkno_map_orig);
	while (hash_map_it2_next(&it))
	{
		struct block *block = it.val;
		if (block->cow_blkno)
		{
			unalloc_block(block->cow_blkno);
			unfree_block(block->orig_blkno);

			(void) hash_map_erase(blkno_map_cow, u64_ptr(block->cow_blkno));
			free(block->dram);
		}

		(void) hash_map_erase(blkno_map_orig, u64_ptr(block->orig_blkno));
		free(block);
	}
}


uint64_t get_super_blockno(void)
{
	struct block *super = hash_map_find_val(blkno_map_orig,
	                                        u64_ptr(BPFS_BLOCKNO_SUPER));
	if (!super || super->cow_blkno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_SUPER;
	return super->cow_blkno;
}

#else

int indirect_cow_init(void)
{
	return 0;
}
void indirect_cow_destroy(void)
{
}

int indirect_cow_parent_push(uint64_t blkno)
{
	return 0;
}
void indirect_cow_parent_pop(uint64_t blkno)
{
}

int indirect_cow_block_cow(uint64_t orig_blkno, uint64_t cow_blkno)
{
	return 0;
}
char* indirect_cow_block_get(uint64_t blkno)
{
	return NULL;
}
void indirect_cow_block_required(uint64_t blkno)
{
}
void indirect_cow_block_direct(uint64_t blkno, unsigned off, unsigned size)
{
}

void indirect_cow_commit(void)
{
}
void indirect_cow_abort(void)
{
}

uint64_t get_super_blockno(void)
{
	return BPFS_BLOCKNO_SUPER;
}

#endif
