/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef BPFS_H
#define BPFS_H

#include "bpfs_structs.h"

#include <stdbool.h>
#include <stdint.h>

#define MODE_SP 1
#define MODE_SCSP 2
#define MODE_BPFS 3

#define COMMIT_MODE MODE_BPFS

// Allow in-place append writes
#define SCSP_OPT_APPEND (1 && COMMIT_MODE == MODE_SCSP)
// Write [acm]time independently of the commit
#define SCSP_OPT_TIME (1 && COMMIT_MODE == MODE_SCSP)

#define APPEASE_VALGRIND 0
// Detect when an inode is used that should no longer be linked into any dir.
// NOTE: This causes additional writes.
#define DETECT_ZEROLINKS_WITH_LINKS (0 && !defined(NDEBUG))

#define SCSP_OPT_DIRECT (SCSP_OPT_APPEND || SCSP_OPT_TIME)
#define INDIRECT_COW (COMMIT_MODE == MODE_SCSP)

// TODO: rephrase this as you-see-everything-p?
// NOTE: this doesn't describe situations where the top block is already COWed
//       but child blocks are refed by the original top block.
enum commit {
	COMMIT_NONE,   // no writes allowed
	COMMIT_COPY,   // writes only to copies
#if COMMIT_MODE == MODE_BPFS
	COMMIT_ATOMIC, // write in-place if write is atomic; otherwise, copy
#else
	COMMIT_ATOMIC = COMMIT_COPY,
#endif
	COMMIT_FREE,   // no restrictions on writes (e.g., region is not yet refed)
};

// Max size that can be written atomically (hardcoded for unsafe 32b testing)
#define ATOMIC_SIZE 8

#define BPFS_EOF UINT64_MAX

uint64_t cow_block(uint64_t old_blockno,
                   unsigned off, unsigned size, unsigned valid);
uint64_t cow_block_hole(unsigned off, unsigned size, unsigned valid);
uint64_t cow_block_entire(uint64_t old_blockno);

#if COMMIT_MODE != MODE_BPFS
bool block_freshly_alloced(uint64_t blockno);
#endif

uint64_t tree_max_nblocks(uint64_t height);
uint64_t tree_height(uint64_t nblocks);
int tree_change_height(struct bpfs_tree_root *root,
                       unsigned new_height,
                       enum commit commit, uint64_t *blockno);

void set_super(struct bpfs_super *super);
struct bpfs_super*  get_bpram_super(void);
struct bpfs_super* get_super(void);
#if COMMIT_MODE == MODE_SCSP
uint64_t get_super_blockno(void);
#endif

char* get_block(uint64_t blockno);
static __inline
unsigned block_offset(const void *x) __attribute__((always_inline));
void unfree_block(uint64_t blockno);
void unalloc_block(uint64_t blockno);

struct bpfs_tree_root* get_inode_root(void);
int get_inode_offset(uint64_t ino, uint64_t *poffset);

void ha_set_addr(struct height_addr *pha, uint64_t addr);
void ha_set(struct height_addr *pha, uint64_t height, uint64_t addr);

uint64_t tree_root_height(const struct bpfs_tree_root *root);
uint64_t tree_root_addr(const struct bpfs_tree_root *root);

int truncate_block_zero(struct bpfs_tree_root *root,
                        uint64_t begin, uint64_t end, uint64_t valid,
                        uint64_t *blockno);


static __inline
unsigned block_offset(const void *x)
{
	return ((uintptr_t) x) % BPFS_BLOCK_SIZE;
}

#endif
