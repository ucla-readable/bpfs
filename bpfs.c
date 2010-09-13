/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "mkbpfs.h"
#include "bpfs_structs.h"
#include "dcache.h"
#include "crawler.h"
#include "indirect_cow.h"
#include "util.h"
#include "hash_map.h"

#define FUSE_USE_VERSION FUSE_MAKE_VERSION(2, 8)
#include <fuse/fuse_lowlevel.h>

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

// TODO:
// - make time higher resolution. See ext4, bits/stat.h, linux/time.h.
// - add gcc flag to warn about ignored return values?
// - blockno can mean bpram block no or file block no
// - tell valgrind about block and inode alloc and free functions
// - merge and breakup empty dirents
// - don't reuse inflight resources? (work with epoch_barrier()?)
// - write fsck.bpfs?: check .., check nlinks, more?
// - can compiler reorder memory writes? watch out for SP and SCSP.
// - how much simpler would it be to always have a correct height tree?
// - passing size=1 to crawl(!COMMIT_NONE) forces extra writes

#define DETECT_ALLOCATION_DIFFS (!defined(NDEBUG))
#define DETECT_NONCOW_WRITES_SP (COMMIT_MODE == MODE_SP && !defined(NDEBUG))
#define DETECT_NONCOW_WRITES_SCSP \
	(COMMIT_MODE == MODE_SCSP && !SCSP_OPT_DIRECT && !defined(NDEBUG))
// Alternatives to valgrind until it knows about our block alloc functions:
// FIXME: broken with SCSP at the moment
#define DETECT_STRAY_ACCESSES (COMMIT_MODE == MODE_SP && !defined(NDEBUG))
#define BLOCK_POISON (0 && !defined(NDEBUG))

// STDTIMEOUT is not 0 because of a fuse kernel module bug.
// Miklos's 2006/06/27 email, E1FvBX0-0006PB-00@dorka.pomaz.szeredi.hu, fixes.
#define STDTIMEOUT 1.0

// Maximum interval between two random fscks. Unit is microseconds.
#define RFSCK_MAX_INTERVAL 100000

#define FUSE_ERR_SUCCESS 0
#define FUSE_BIG_WRITES (FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8))

// Offset of the first persistent dirent. Offset 0 is "." and 1 is "..".
#define DIRENT_FIRST_PERSISTENT_OFFSET 2

// Use this macro to ensure that memory writes are made inbetween calls to
// this macro. With hardware support this would also issue an epoch barrier.
#define epoch_barrier() __asm__ __volatile__("": : :"memory")

#define DEBUG (0 && !defined(NDEBUG))
#if DEBUG
# define Dprintf(x...) fprintf(stderr, x)
#else
# define Dprintf(x...) do {} while(0)
#endif

#define DEBUG_BLOCKS (0 && !defined(NDEBUG))
#if DEBUG_BLOCKS
# define DBprintf(x...) fprintf(stderr, x)
#else
# define DBprintf(x...) do {} while(0)
#endif


#define DEBUG_INODES (0 && !defined(NDEBUG))
#if DEBUG_INODES
# define DIprintf(x...) fprintf(stderr, x)
#else
# define DIprintf(x...) do {} while(0)
#endif

#if DETECT_NONCOW_WRITES_SP
# define PROT_INUSE_OLD PROT_READ
#else
# define PROT_INUSE_OLD (PROT_READ | PROT_WRITE)
#endif

struct const_str
{
	const char *str;
	size_t len;
};

struct str_dirent
{
	struct const_str str;       /* find the dirent with this name */
	uint64_t dirent_off;        /* set if str is found */
	struct bpfs_dirent *dirent; /* set if str is found */
};


//
// give access to bpram without passing these variables everywhere

static char *bpram;
static size_t bpram_size;

static struct bpfs_super *bpfs_super;

void set_super(struct bpfs_super *super)
{
	assert(super->magic == BPFS_FS_MAGIC);
	bpfs_super = super;
}

struct bpfs_super* get_bpram_super(void)
{
	static_assert(BPFS_BLOCKNO_INVALID == 0 && BPFS_BLOCKNO_SUPER == 1);
	return (struct bpfs_super*) bpram;
}

struct bpfs_super* get_super(void)
{
	return bpfs_super;
}


//
// BPFS-FUSE type conversion

static mode_t b2f_filetype(uint32_t bpfs_file_type)
{
	mode_t mode = 0;
	switch (bpfs_file_type)
	{
		case BPFS_TYPE_SOCK:
			mode |= S_IFSOCK;
			break;
		case BPFS_TYPE_FILE:
			mode |= S_IFREG;
			break;
		case BPFS_TYPE_BLKDEV:
			mode |= S_IFBLK;
			break;
		case BPFS_TYPE_DIR:
			mode |= S_IFDIR;
			break;
		case BPFS_TYPE_CHRDEV:
			mode |= S_IFCHR;
			break;
		case BPFS_TYPE_FIFO:
			mode |= S_IFIFO;
			break;
		case BPFS_TYPE_SYMLINK:
			mode |= S_IFLNK;
			break;
		default:
			xassert(0);
	}
	return mode;
}

static uint32_t f2b_filetype(mode_t fuse_mode)
{
	uint32_t file_type = 0;
	switch (fuse_mode & S_IFMT)
	{
		case S_IFSOCK:
			file_type |= BPFS_TYPE_SOCK;
			break;
		case S_IFREG:
			file_type |= BPFS_TYPE_FILE;
			break;
		case S_IFBLK:
			file_type |= BPFS_TYPE_BLKDEV;
			break;
		case S_IFDIR:
			file_type |= BPFS_TYPE_DIR;
			break;
		case S_IFCHR:
			file_type |= BPFS_TYPE_CHRDEV;
			break;
		case S_IFIFO:
			file_type |= BPFS_TYPE_FIFO;
			break;
		case S_IFLNK:
			file_type |= BPFS_TYPE_SYMLINK;
			break;
		default:
			xassert(0);
	}
	return file_type;
}

static mode_t b2f_mode(uint32_t bmode)
{
	mode_t fmode = 0;
	switch (bmode & BPFS_S_IFMT)
	{
		case BPFS_S_IFSOCK: fmode |= S_IFSOCK; break;
		case BPFS_S_IFLNK:  fmode |= S_IFLNK;  break;
		case BPFS_S_IFREG:  fmode |= S_IFREG;  break;
		case BPFS_S_IFBLK:  fmode |= S_IFBLK;  break;
		case BPFS_S_IFDIR:  fmode |= S_IFDIR;  break;
		case BPFS_S_IFCHR:  fmode |= S_IFCHR;  break;
		case BPFS_S_IFIFO:  fmode |= S_IFIFO;  break;
		default: xassert(0);
	}
	fmode |= bmode & BPFS_S_IPERM;
	return fmode;
}

static uint32_t f2b_mode(mode_t fmode)
{
	uint32_t bmode = 0;
	switch (fmode & S_IFMT)
	{
		case S_IFSOCK: bmode |= BPFS_S_IFSOCK; break;
		case S_IFLNK:  bmode |= BPFS_S_IFLNK;  break;
		case S_IFREG:  bmode |= BPFS_S_IFREG;  break;
		case S_IFBLK:  bmode |= BPFS_S_IFBLK;  break;
		case S_IFDIR:  bmode |= BPFS_S_IFDIR;  break;
		case S_IFCHR:  bmode |= BPFS_S_IFCHR;  break;
		case S_IFIFO:  bmode |= BPFS_S_IFIFO;  break;
		default: xassert(0);
	}
	bmode |= fmode & BPFS_S_IPERM;
	return bmode;
}


//
// bitmap

struct staged_entry {
	uint64_t index;
	struct staged_entry *next;
};

static void staged_list_free(struct staged_entry **head)
{
	while (*head)
	{
		struct staged_entry *cur = *head;
		*head = (*head)->next;
		free(cur);
	}
}

#if (COMMIT_MODE != MODE_BPFS) || !defined(NDEBUG)
static bool staged_list_freshly_alloced(struct staged_entry *head, uint64_t no)
{
	struct staged_entry *entry;
	for (entry = head; entry; entry = entry->next)
		if (no == entry->index)
			return true;
	return false;
}
#endif


struct bitmap {
	char *bitmap;
	uint64_t ntotal;
	uint64_t nfree;
	struct staged_entry *allocs;
	struct staged_entry *frees;
	uint64_t prev_ntotal;
};

#define BITMAP_SCAN_MAX UINT64_MAX

static int bitmap_init(struct bitmap *bitmap, uint64_t ntotal)
{
	size_t size = ntotal / 8;
	xassert(!(ntotal % (sizeof(bitmap_scan_t) * 8)));
	assert(!bitmap->bitmap);
	bitmap->bitmap = malloc(size);
	if (!bitmap->bitmap)
		return -ENOMEM;
	memset(bitmap->bitmap, 0, size);
	bitmap->nfree = bitmap->ntotal = ntotal;
	bitmap->allocs = bitmap->frees = NULL;
	bitmap->prev_ntotal = 0;
	return 0;
}

static void bitmap_destroy(struct bitmap *bitmap)
{
	free(bitmap->bitmap);
	bitmap->bitmap = NULL;
	staged_list_free(&bitmap->allocs);
	staged_list_free(&bitmap->frees);
}

static void bitmap_move(struct bitmap *dst, struct bitmap *org)
{
	memcpy(dst, org, sizeof(*dst));

	org->bitmap = NULL;
	org->ntotal = 0;
	org->nfree = 0;
	org->allocs = NULL;
	org->frees = NULL;
	org->prev_ntotal = 0;
}

static int bitmap_resize(struct bitmap *bitmap, uint64_t ntotal)
{
	char *new_bitmap;

	if (bitmap->ntotal == ntotal)
		return 0;

#ifndef NDEBUG
	if (bitmap->ntotal > ntotal)
	{
		uint64_t i;
		assert(!(ntotal % (sizeof(bitmap_scan_t) * 8)));
		assert(!(bitmap->ntotal % (sizeof(bitmap_scan_t) * 8)));
		for (i = ntotal; i < bitmap->ntotal; i += sizeof(bitmap_scan_t) * 8)
			assert(!((bitmap_scan_t*) bitmap->bitmap)[i / 8]);
	}
#endif

	new_bitmap = realloc(bitmap->bitmap, ntotal);
	if (!new_bitmap)
		return -ENOMEM;
	bitmap->bitmap = new_bitmap;

	if (!bitmap->prev_ntotal)
		bitmap->prev_ntotal = bitmap->ntotal;

	if (bitmap->ntotal < ntotal)
	{
		uint64_t delta = ntotal - bitmap->ntotal;
		memset(bitmap->bitmap + bitmap->ntotal / 8, 0, delta / 8);
		bitmap->nfree += delta;
		bitmap->ntotal = ntotal;
	}
	else
	{
		uint64_t delta = bitmap->ntotal - ntotal;

		// easier if these are NULL
		assert(!bitmap->allocs);
		assert(!bitmap->frees);

		bitmap->nfree -= delta;
		bitmap->ntotal = ntotal;
	}

	return 0;
}

static uint64_t bitmap_alloc(struct bitmap *bitmap)
{
	uint64_t i;
	for (i = 0; i < bitmap->ntotal; i += sizeof(bitmap_scan_t) * 8)
	{
		bitmap_scan_t *word = (bitmap_scan_t*) (bitmap->bitmap + i / 8);
		if (*word != BITMAP_SCAN_MAX)
		{
			int j;
			for (j = 0; j < sizeof(*word) * 8; j++)
			{
				if (!(*word & (((bitmap_scan_t) 1) << j)))
				{
					struct staged_entry *found = malloc(sizeof(*found));
					xassert(found); // No way to return non-ENOSPC error
					found->index = i + j;
					found->next = bitmap->allocs;
					bitmap->allocs = found;
					*word |= ((bitmap_scan_t) 1) << j;
					bitmap->nfree--;
					return found->index;
				}
			}
		}
	}
	return bitmap->ntotal;
}

static void bitmap_set(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;
	assert(no < bitmap->ntotal);
	assert(!(*word & (1 << (no % 8))));
	*word |= (1 << (no % 8));
	bitmap->nfree--;
}

static bool bitmap_ensure_set(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;
	bool was_set;
	assert(no < bitmap->ntotal);
	was_set = *word & (1 << (no % 8));
	*word |= (1 << (no % 8));
	if (!was_set)
		bitmap->nfree--;
	return was_set;
}

static void bitmap_free(struct bitmap *bitmap, uint64_t no)
{
	struct staged_entry *staged = malloc(sizeof(*staged));

	assert(no < bitmap->ntotal);
	assert(bitmap->bitmap[no / 8] & (1 << (no % 8)));
	assert(!staged_list_freshly_alloced(bitmap->frees, no));

	xassert(staged); // TODO/FIXME: recover

	staged->index = no;
	staged->next = bitmap->frees;
	bitmap->frees = staged;
}

static void bitmap_unfree(struct bitmap *bitmap, uint64_t no)
{
	struct staged_entry *pstaged = NULL;
	struct staged_entry *staged = bitmap->frees;
	bool was_set;

	assert(no < bitmap->ntotal);
	assert(bitmap->bitmap[no / 8] & (1 << (no % 8)));

	for (; staged; pstaged = staged, staged = staged->next)
		if (no == staged->index)
			break;
	xassert(staged);

	if (!pstaged)
		bitmap->frees = staged->next;
	else
		pstaged->next = staged->next;
	free(staged);

	was_set = bitmap_ensure_set(bitmap, no);
	assert(was_set);
}

static void bitmap_clear(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;

	assert(no < bitmap->ntotal);
	assert(*word & (1 << (no % 8)));

	*word &= ~(1 << (no % 8));
	bitmap->nfree++;
}

static void bitmap_unalloc(struct bitmap *bitmap, uint64_t no)
{
	struct staged_entry *pstaged = NULL;
	struct staged_entry *staged = bitmap->allocs;

	assert(no < bitmap->ntotal);
	assert(bitmap->bitmap[no / 8] & (1 << (no % 8)));

	for (; staged; pstaged = staged, staged = staged->next)
		if (no == staged->index)
			break;
	xassert(staged);

	if (!pstaged)
		bitmap->allocs = staged->next;
	else
		pstaged->next = staged->next;
	free(staged);

	bitmap_clear(bitmap, no);
}

static void bitmap_abort(struct bitmap *bitmap)
{
	while (bitmap->allocs)
	{
		struct staged_entry *cur = bitmap->allocs;
		bitmap_clear(bitmap, cur->index);
		bitmap->allocs = bitmap->allocs->next;
		free(cur);
	}

	staged_list_free(&bitmap->frees);

	if (bitmap->prev_ntotal && bitmap->ntotal != bitmap->prev_ntotal)
	{
		uint64_t prev_ntotal = bitmap->prev_ntotal;
		int r;
		r = bitmap_resize(bitmap, prev_ntotal);
		xassert(r >= 0); // TODO: recover
	}
	bitmap->prev_ntotal = 0;
}

static void bitmap_commit(struct bitmap *bitmap)
{
	staged_list_free(&bitmap->allocs);

	while (bitmap->frees)
	{
		struct staged_entry *cur = bitmap->frees;
		bitmap_clear(bitmap, cur->index);
		bitmap->frees = bitmap->frees->next;
		free(cur);
	}

	bitmap->prev_ntotal = 0;
}

#if DETECT_STRAY_ACCESSES
// Limit the define only because the function is otherwise not referenced
static bool bitmap_is_alloced(const struct bitmap *bitmap, uint64_t no)
{
	assert(no < bitmap->ntotal);
	// right now this function is only used with no outstanding allocs or frees
	assert(!bitmap->allocs);
	assert(!bitmap->frees);
	return bitmap->bitmap[no / 8] & (1 << (no % 8));
}
#endif


//
// block allocation

struct block_allocation {
	struct bitmap bitmap;
};

static struct block_allocation block_alloc;

static int init_block_allocations(void)
{
	return bitmap_init(&block_alloc.bitmap, bpfs_super->nblocks);
}

static void destroy_block_allocations(void)
{
	bitmap_destroy(&block_alloc.bitmap);
}

static void move_block_allocations(struct block_allocation *dst, struct block_allocation *org)
{
	bitmap_move(&dst->bitmap, &org->bitmap);
}

#if BLOCK_POISON
static void poison_block(uint64_t blockno)
{
	uint32_t *block = (uint32_t*) get_block(blockno);
	unsigned i;
	for (i = 0; i < BPFS_BLOCK_SIZE / sizeof(*block); i++)
		block[i] = 0xdeadbeef;
}
#endif

static uint64_t alloc_block(void)
{
	uint64_t no = bitmap_alloc(&block_alloc.bitmap);
	DBprintf("%s() = %" PRIu64 "\n", __FUNCTION__, no + 1);
	if (no == block_alloc.bitmap.ntotal)
		return BPFS_BLOCKNO_INVALID;
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	assert(no + 1 >= BPFS_BLOCKNO_FIRST_ALLOC);
#if (DETECT_STRAY_ACCESSES || DETECT_NONCOW_WRITES_SP || DETECT_NONCOW_WRITES_SCSP)
	xsyscall(mprotect(get_block(no + 1), BPFS_BLOCK_SIZE, PROT_READ | PROT_WRITE));
#endif
#if BLOCK_POISON
	poison_block(no + 1);
#endif
	return no + 1;
}

void unfree_block(uint64_t blockno)
{
	DBprintf("%s() = %" PRIu64 "\n", __FUNCTION__, blockno);
	assert(blockno != BPFS_BLOCKNO_INVALID);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_unfree(&block_alloc.bitmap, blockno - 1);
#if (DETECT_STRAY_ACCESSES || DETECT_NONCOW_WRITES_SP)
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_READ | PROT_WRITE));
#endif
#if BLOCK_POISON
	poison_block(blockno);
#endif
}

#if COMMIT_MODE != MODE_BPFS
bool block_freshly_alloced(uint64_t blockno)
{
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return staged_list_freshly_alloced(block_alloc.bitmap.allocs, blockno - 1);
}
#endif

static void set_block(uint64_t blockno)
{
	DBprintf("%s() = %" PRIu64 "\n", __FUNCTION__, blockno);
	assert(blockno != BPFS_BLOCKNO_INVALID);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_set(&block_alloc.bitmap, blockno - 1);
}

static void free_block(uint64_t blockno)
{
	DBprintf("%s() = %" PRIu64 "\n", __FUNCTION__, blockno);
	assert(blockno != BPFS_BLOCKNO_INVALID);
#if INDIRECT_COW
	// indirect_cow may temporarily free the super block
	assert(blockno == BPFS_BLOCKNO_SUPER
	       || blockno >= BPFS_BLOCKNO_FIRST_ALLOC);
#else
	assert(blockno >= BPFS_BLOCKNO_FIRST_ALLOC);
#endif
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_free(&block_alloc.bitmap, blockno - 1);
#if DETECT_STRAY_ACCESSES
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_NONE));
#else
# if BLOCK_POISON
	poison_block(blockno);
# endif
# if DETECT_NONCOW_WRITES_SP
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_READ));
# endif
#endif
}

void unalloc_block(uint64_t blockno)
{
	DBprintf("%s() = %" PRIu64 "\n", __FUNCTION__, blockno);
	assert(blockno != BPFS_BLOCKNO_INVALID);
	assert(blockno >= BPFS_BLOCKNO_FIRST_ALLOC);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_unalloc(&block_alloc.bitmap, blockno - 1);
#if DETECT_STRAY_ACCESSES
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_NONE));
#else
# if BLOCK_POISON
	poison_block(blockno);
# endif
# if DETECT_NONCOW_WRITES_SP
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_READ));
# endif
#endif
}

static void protect_bpram_abort(void)
{
#if DETECT_STRAY_ACCESSES
	struct staged_entry *cur;
	for (cur = block_alloc.bitmap.frees; cur; cur = cur->next)
		xsyscall(mprotect(bpram + cur->index * BPFS_BLOCK_SIZE,
		                  BPFS_BLOCK_SIZE, PROT_INUSE_OLD));
	for (cur = block_alloc.bitmap.allocs; cur; cur = cur->next)
		xsyscall(mprotect(bpram + cur->index * BPFS_BLOCK_SIZE,
		                  BPFS_BLOCK_SIZE, PROT_NONE));
#elif DETECT_NONCOW_WRITES_SP
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
#endif
}

static void protect_bpram_commit(void)
{
#if DETECT_NONCOW_WRITES_SP
# if DETECT_STRAY_ACCESSES
	struct staged_entry *cur;
	for (cur = block_alloc.bitmap.allocs; cur; cur = cur->next)
		xsyscall(mprotect(bpram + cur->index * BPFS_BLOCK_SIZE,
		                  BPFS_BLOCK_SIZE, PROT_READ));
# else
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
# endif
#endif
}

static void abort_blocks(void)
{
	protect_bpram_abort();
	bitmap_abort(&block_alloc.bitmap);
}

static void commit_blocks(void)
{
	protect_bpram_commit();
	bitmap_commit(&block_alloc.bitmap);
}

char* get_block(uint64_t blockno)
{
#if INDIRECT_COW
	char *block;
#endif

	if (blockno == BPFS_BLOCKNO_INVALID)
	{
		assert(0);
		return NULL;
	}
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	if (blockno > bpfs_super->nblocks)
	{
		assert(0);
		return NULL;
	}

#if INDIRECT_COW
	if ((block = indirect_cow_block_get(blockno)))
	{
		assert(!block_offset(block));
		return block;
	}
#endif
	return bpram + (blockno - 1) * BPFS_BLOCK_SIZE;
}

#if DETECT_STRAY_ACCESSES
// Limit the define only because the function is otherwise not referenced
static bool block_is_alloced(uint64_t blockno)
{
	xassert(blockno != BPFS_BLOCKNO_INVALID);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return bitmap_is_alloced(&block_alloc.bitmap, blockno - 1);
}
#endif


//
// block utility functions

static uint64_t cow_nbytes;
static uint64_t cow_nblocks;

uint64_t cow_block(uint64_t old_blockno,
                   unsigned off, unsigned size, unsigned valid)
{
	uint64_t new_blockno;
	char *old_block;
	char *new_block;
	int r;
	uint64_t end = off + size;

	assert(off + size <= BPFS_BLOCK_SIZE);
	assert(valid <= BPFS_BLOCK_SIZE);

#if COMMIT_MODE != MODE_BPFS
	if (block_freshly_alloced(old_blockno))
		return old_blockno;
#endif

	new_blockno = alloc_block();
	if (new_blockno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_INVALID;
#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(get_block(new_blockno), BPFS_BLOCK_SIZE, PROT_READ));
#endif
	r = indirect_cow_block_cow(old_blockno, new_blockno);
	if (r < 0)
	{
		unalloc_block(new_blockno);
		set_block(old_blockno);
		return BPFS_BLOCKNO_INVALID;
	}

	old_block = get_block(old_blockno);
	new_block = get_block(new_blockno);
	memcpy(new_block, old_block, off);
	cow_nbytes += off;
	if (end < valid)
	{
		memcpy(new_block + end, old_block + end, valid - end);
		cow_nbytes += valid - end;
	}
	if (off || end < valid)
		cow_nblocks++;
	free_block(old_blockno);
	return new_blockno;
}

uint64_t cow_block_hole(unsigned off, unsigned size, unsigned valid)
{
	uint64_t blockno;
	char *block;
	uint64_t end = off + size;

	assert(off + size <= BPFS_BLOCK_SIZE);
	assert(valid <= BPFS_BLOCK_SIZE);

	blockno = alloc_block();
	if (blockno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_INVALID;
	// DETECT_NONCOW_WRITES_SCSP: do not mark read-only; no dram copy

	block = get_block(blockno);
	memset(block, 0, off);
	if (end < valid)
		memset(block + end, 0, valid - end);
	return blockno;
}

uint64_t cow_block_entire(uint64_t old_blockno)
{
	uint64_t new_blockno;
	char *old_block;
	char *new_block;
	int r;

#if COMMIT_MODE != MODE_BPFS
	if (block_freshly_alloced(old_blockno))
		return old_blockno;
#endif

	new_blockno = alloc_block();
	if (new_blockno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_INVALID;
#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(get_block(new_blockno), BPFS_BLOCK_SIZE, PROT_READ));
#endif
	r = indirect_cow_block_cow(old_blockno, new_blockno);
	if (r < 0)
	{
		unalloc_block(new_blockno);
		set_block(old_blockno);
		return BPFS_BLOCKNO_INVALID;
	}

	old_block = get_block(old_blockno);
	new_block = get_block(new_blockno);
	memcpy(new_block, old_block, BPFS_BLOCK_SIZE);
	cow_nbytes += BPFS_BLOCK_SIZE;
	cow_nblocks++;
	free_block(old_blockno);
	return new_blockno;
}

static void callback_truncate_block_free(uint64_t blockno, bool leaf)
{
	free_block(blockno);
}

static void truncate_block_free(struct bpfs_tree_root *root, uint64_t new_size)
{
	uint64_t off = ROUNDUP64(new_size, BPFS_BLOCK_SIZE);
	if (off < root->nbytes)
		crawl_blocknos(root, off, BPFS_EOF, callback_truncate_block_free);
}


//
// inode allocation

struct bpfs_tree_root* get_inode_root(void)
{
	return (struct bpfs_tree_root*) get_block(bpfs_super->inode_root_addr);
}

struct inode_allocation {
	struct bitmap bitmap;
};

static struct inode_allocation inode_alloc;

static int init_inode_allocations(void)
{
	struct bpfs_tree_root *inode_root = get_inode_root();

	// This code assumes that inodes are contiguous in the inode tree
	static_assert(!(BPFS_BLOCK_SIZE % sizeof(struct bpfs_inode)));

	return bitmap_init(&inode_alloc.bitmap,
	                   NBLOCKS_FOR_NBYTES(inode_root->nbytes)
	                   * BPFS_INODES_PER_BLOCK);
}

static void destroy_inode_allocations(void)
{
	bitmap_destroy(&inode_alloc.bitmap);
}

static void move_inode_allocations(struct inode_allocation *dst, struct inode_allocation *org)
{
	bitmap_move(&dst->bitmap, &org->bitmap);
}

static int callback_init_inodes(uint64_t blockoff, char *block,
                                unsigned off, unsigned size, unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *user, uint64_t *blockno)
{
#if APPEASE_VALGRIND || DETECT_ZEROLINKS_WITH_LINKS
	assert(!(off % sizeof(struct bpfs_inode)));
	for (; off + sizeof(struct bpfs_inode) <= size; off += sizeof(struct bpfs_inode))
	{
		struct bpfs_inode *inode = (struct bpfs_inode*) (block + off);
# if APPEASE_VALGRIND
		// init the generation field. not required, but appeases valgrind.
		inode->generation = 0;
# endif
# if DETECT_ZEROLINKS_WITH_LINKS
		inode->nlinks = 0;
# endif
	}
#endif
	return 0;
}

static struct bpfs_inode* get_inode(uint64_t ino);

static uint64_t alloc_inode(void)
{
	uint64_t no = bitmap_alloc(&inode_alloc.bitmap);
	if (no == inode_alloc.bitmap.ntotal)
	{
		struct bpfs_tree_root *inode_root = get_inode_root();
		if (crawl_inodes(inode_root->nbytes, inode_root->nbytes,
		                 COMMIT_ATOMIC, callback_init_inodes, NULL) < 0)
			return BPFS_INO_INVALID;
		inode_root = get_inode_root();
		xcall(bitmap_resize(&inode_alloc.bitmap,
		                    inode_root->nbytes / sizeof(struct bpfs_inode)));
		no = bitmap_alloc(&inode_alloc.bitmap);
		assert(no != inode_alloc.bitmap.ntotal);
	}
	static_assert(BPFS_INO_INVALID == 0);
#if DETECT_ZEROLINKS_WITH_LINKS
	assert(!get_inode(no + 1)->nlinks);
#endif
	DIprintf("%s() -> ino %" PRIu64 "\n", __FUNCTION__, no + 1);
	return no + 1;
}

static bool set_inode(uint64_t ino)
{
	assert(ino != BPFS_INO_INVALID);
	static_assert(BPFS_INO_INVALID == 0);
	return bitmap_ensure_set(&inode_alloc.bitmap, ino - 1);
}

static void free_inode(uint64_t ino)
{
	assert(ino != BPFS_INO_INVALID);
	DIprintf("%s(ino = %" PRIu64 ")\n", __FUNCTION__, ino);
	static_assert(BPFS_INO_INVALID == 0);
	bitmap_free(&inode_alloc.bitmap, ino - 1);
}

static void abort_inodes(void)
{
	bitmap_abort(&inode_alloc.bitmap);
}

static void commit_inodes(void)
{
	bitmap_commit(&inode_alloc.bitmap);
}

int get_inode_offset(uint64_t ino, uint64_t *poffset)
{
	uint64_t no;
	uint64_t offset;

	if (ino == BPFS_INO_INVALID)
	{
		assert(0);
		return -EINVAL;
	}

	static_assert(BPFS_INO_INVALID == 0);
	no = ino - 1;

	if (no >= inode_alloc.bitmap.ntotal)
	{
		assert(0);
		return -EINVAL;
	}

	offset = no * sizeof(struct bpfs_inode);
	if (offset + sizeof(struct bpfs_inode) > get_inode_root()->nbytes)
	{
		assert(0);
		return -EINVAL;
	}
	*poffset = offset;
	return 0;
}

static int callback_get_inode(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *pinode_void, uint64_t *blockno)
{
	struct bpfs_inode **pinode = pinode_void;
	*pinode = inode;
	return 0;
}

static struct bpfs_inode* get_inode(uint64_t ino)
{
	struct bpfs_inode *inode;
	xcall(crawl_inode(ino, COMMIT_NONE, callback_get_inode, &inode));
	return inode;
}


//
// misc internal functions

/* Return whether a write can be made atomically.
 * 'offset': byte offset into the block.
 * 'size': number of bytes to write.
 */
static bool can_atomic_write(unsigned offset, unsigned size)
{
	unsigned last = offset + size - 1;
	return last - offset < ATOMIC_SIZE
	       && (offset % ATOMIC_SIZE) <= (last % ATOMIC_SIZE);
}

static uint64_t tree_nblocks_nblocks;

static void callback_tree_nblocks(uint64_t blockno, bool leaf)
{
	assert(blockno != BPFS_BLOCKNO_INVALID);
	if (leaf)
		tree_nblocks_nblocks++;
}

static uint64_t tree_nblocks(const struct bpfs_tree_root *root)
{
	uint64_t nblocks;

	assert(!tree_nblocks_nblocks);

	crawl_blocknos(root, 0, BPFS_EOF, callback_tree_nblocks);

	nblocks = tree_nblocks_nblocks;
	tree_nblocks_nblocks = 0;
	return nblocks;
}

#if (COMMIT_MODE == MODE_SP) && !defined(NDEBUG)
// Limit the define only because the function is otherwise not referenced
static uint64_t bpram_blockno(const void *x)
{
	const char *c = (const char*) x;
	assert(bpram <= c && c < bpram + bpram_size);
	if (c < bpram || bpram + bpram_size <= c)
		return BPFS_BLOCKNO_INVALID;
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return (((uintptr_t) (c - bpram)) / BPFS_BLOCK_SIZE) + 1;
}
#endif

static int bpfs_stat(fuse_ino_t ino, struct stat *stbuf)
{
	struct bpfs_inode *inode = get_inode(ino);
	if (!inode)
		return -ENOENT;
	assert(inode->nlinks);
	memset(stbuf, 0, sizeof(stbuf));
	/* stbuf->st_dev */
	stbuf->st_ino = ino;
	stbuf->st_nlink = inode->nlinks;
	stbuf->st_mode = b2f_mode(inode->mode);
	stbuf->st_uid = inode->uid;
	stbuf->st_gid = inode->gid;
	/* stbuf->st_rdev */
	stbuf->st_size = inode->root.nbytes;
	stbuf->st_blksize = BPFS_BLOCK_SIZE;
	stbuf->st_blocks = tree_nblocks(&inode->root) * BPFS_BLOCK_SIZE / 512;
	stbuf->st_atime = inode->atime.sec;
	stbuf->st_atim.tv_nsec = 0; // TODO: read nsec from inode
	stbuf->st_mtime = inode->mtime.sec;
	stbuf->st_mtim.tv_nsec = 0; // TODO: read nsec from inode
	stbuf->st_ctime = inode->ctime.sec;
	stbuf->st_ctim.tv_nsec = 0; // TODO: read nsec from inode
	return 0;
}

static void mdirent_init_dirent(struct mdirent *md,
                          const struct bpfs_dirent *d, uint64_t off)
{
	mdirent_init(md, d->name, off, d->ino, get_inode(d->ino)->generation,
	             d->rec_len, d->file_type);
}


//
// atomic setters for struct height_addr

void ha_set_addr(struct height_addr *pha, uint64_t addr)
{
	struct height_addr ha = { .height = pha->height, .addr = addr };
	assert(addr <= BPFS_TREE_ROOT_MAX_ADDR);
	*pha = ha;
}

void ha_set(struct height_addr *pha, uint64_t height, uint64_t addr)
{
	struct height_addr ha = { .height = height, .addr = addr };
	assert(height <= BPFS_TREE_MAX_HEIGHT);
	assert(addr <= BPFS_TREE_ROOT_MAX_ADDR);
	*pha = ha;
}


//
// tree functions

uint64_t tree_max_nblocks(uint64_t height)
{
	uint64_t max_nblocks = 1;
	while (height--)
		max_nblocks *= BPFS_BLOCKNOS_PER_INDIR;
	return max_nblocks;
}

uint64_t tree_height(uint64_t nblocks)
{
	uint64_t height = 0;
	uint64_t max_nblocks = 1;
	while (nblocks > max_nblocks)
	{
		max_nblocks *= BPFS_BLOCKNOS_PER_INDIR;
		height++;
	}
	return height;
}

int tree_change_height(struct bpfs_tree_root *root,
                       unsigned new_height,
                       enum commit commit, uint64_t *blockno)
{
	uint64_t height = tree_root_height(root);
	uint64_t new_root_addr;

	assert(commit != COMMIT_NONE);

	if (height == new_height)
		return 0;

	if (new_height > height)
	{
		if (root->nbytes && tree_root_addr(root) != BPFS_BLOCKNO_INVALID)
		{
			uint64_t child_max_nbytes = BPFS_BLOCK_SIZE
			                            * tree_max_nblocks(height);
			new_root_addr = tree_root_addr(root);
			for (; height < new_height; height++)
			{
				uint64_t max_nbytes = BPFS_BLOCKNOS_PER_INDIR
				                      * child_max_nbytes;
				uint64_t new_blockno;
				struct bpfs_indir_block *new_indir;

				if ((new_blockno = alloc_block()) == BPFS_BLOCKNO_INVALID)
					return -ENOSPC;
				new_indir = (struct bpfs_indir_block*) get_block(new_blockno);

				new_indir->addr[0] = new_root_addr;

				// If the file was larger than the tree we need to mark the
				//   newly valid block entries as sparse.
				// This is truncate_block_zero(), but simpler.
				if (child_max_nbytes < root->nbytes)
				{
					uint64_t valid = child_max_nbytes;
					uint64_t next_valid = MIN(root->nbytes, max_nbytes);
					int i = 1;
					for (; valid < next_valid; i++, valid += child_max_nbytes)
						new_indir->addr[i] = BPFS_BLOCKNO_INVALID;
				}

				new_root_addr = new_blockno;
				child_max_nbytes = max_nbytes;
			}
		}
		else
			new_root_addr = BPFS_BLOCKNO_INVALID;
	}
	else
	{
		unsigned height_delta = height - new_height;
		new_root_addr = tree_root_addr(root);
		while (height_delta-- && new_root_addr != BPFS_BLOCKNO_INVALID)
		{
			struct bpfs_indir_block *indir = (struct bpfs_indir_block*) get_block(new_root_addr);
			// truncate_block_free() has already freed the block
			new_root_addr = indir->addr[0];
		}
	}

	if (commit == COMMIT_COPY)
	{
		unsigned root_off = block_offset(root);
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		root = (struct bpfs_tree_root*) (get_block(new_blockno) + root_off);
		*blockno = new_blockno;
	}

	ha_set(&root->ha, new_height, new_root_addr);
	return 0;
}

uint64_t tree_root_height(const struct bpfs_tree_root *root)
{
	if (!root->nbytes)
		return BPFS_BLOCKNO_INVALID;
	return root->ha.height;
}

uint64_t tree_root_addr(const struct bpfs_tree_root *root)
{
	if (!root->nbytes)
		return BPFS_BLOCKNO_INVALID;
	return root->ha.addr;
}


//
// block and inode allocation discovery

static void discover_indir_allocations(struct bpfs_indir_block *indir,
                                       unsigned height,
                                       uint64_t max_nblocks,
                                       uint64_t valid)
{
	uint64_t child_max_nblocks = max_nblocks / BPFS_BLOCKNOS_PER_INDIR;
	uint64_t child_max_nbytes = child_max_nblocks * BPFS_BLOCK_SIZE;
	uint64_t lastno = (valid - 1) / (BPFS_BLOCK_SIZE * child_max_nblocks);
	unsigned no;
	for (no = 0; no <= lastno; no++)
	{
		if (indir->addr[no] != BPFS_BLOCKNO_INVALID)
		{
			set_block(indir->addr[no]);
			if (height > 1)
			{
				struct bpfs_indir_block *child_indir = (struct bpfs_indir_block*) get_block(indir->addr[no]);
				uint64_t child_valid;
				if (no < lastno)
					child_valid = child_max_nbytes;
				else
					child_valid = valid - no * child_max_nbytes;
				discover_indir_allocations(child_indir, height - 1,
				                           child_max_nblocks, child_valid);
			}
		}
	}
}

static void discover_tree_allocations(struct bpfs_tree_root *root)
{
	// TODO: better to call crawl_tree()?
	if (tree_root_addr(root) == BPFS_BLOCKNO_INVALID)
		return;

	set_block(tree_root_addr(root));
	if (tree_root_height(root))
	{
		struct bpfs_indir_block *indir =
			(struct bpfs_indir_block*) get_block(tree_root_addr(root));
		uint64_t max_nblocks = tree_max_nblocks(tree_root_height(root));
		discover_indir_allocations(indir, tree_root_height(root), max_nblocks,
		                           root->nbytes);
	}
}

static void discover_inode_allocations(uint64_t ino, bool mounting);

struct mount_ino {
	bool mounting;
	uint64_t ino;
};

static int callback_discover_inodes(uint64_t blockoff, char *block,
                                    unsigned off, unsigned size,
                                    unsigned valid, uint64_t crawl_start,
                                    enum commit commit, void *mi_void,
                                    uint64_t *blockno)
{
	const struct mount_ino *mi = (const struct mount_ino*) mi_void;
	unsigned end = off + size;

	while (off + BPFS_DIRENT_MIN_LEN <= end)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		xassert(off <= BPFS_BLOCK_SIZE);

		if (dirent->ino != BPFS_INO_INVALID)
		{
			discover_inode_allocations(dirent->ino, mi->mounting);

			// Account for child's ".." dirent (not stored on disk):
			if (mi->mounting && !bpfs_super->ephemeral_valid
			    && dirent->file_type == BPFS_TYPE_DIR)
			{
				get_inode(mi->ino)->nlinks++;
				xassert(get_inode(mi->ino)->nlinks);
			}
		}
	}
	return 0;
}

static void discover_inode_allocations(uint64_t ino, bool mounting)
{
	struct bpfs_inode *inode = get_inode(ino);
	struct mount_ino mi = {.mounting = mounting, .ino = ino};
	bool is_dir = BPFS_S_ISDIR(inode->mode);
	bool was_set;

	was_set = set_inode(ino);
	xassert(!is_dir || !was_set); // a dir has only one non-".." dirent

	if (mounting && !bpfs_super->ephemeral_valid)
	{
		inode->nlinks++;
		xassert(inode->nlinks);
		if (is_dir)
		{
			// Account for inode's "." dirent (not stored on disk):
			inode->nlinks++;
			xassert(inode->nlinks);
		}
	}
	else
	{
		if (is_dir)
			xassert(inode->nlinks >= 2);
		else
			xassert(inode->nlinks >= 1);
	}

	if (!was_set)
	{
		// TODO: combine the inode and block discovery loops?
		discover_tree_allocations(&inode->root);
		if (is_dir)
			xcall(crawl_data(ino, 0, BPFS_EOF, COMMIT_NONE,
			                 callback_discover_inodes, &mi));
	}
}

static int callback_reset_inodes_nlinks(uint64_t blockoff, char *block,
                                        unsigned off, unsigned size,
                                        unsigned valid, uint64_t crawl_start,
                                        enum commit commit, void *user,
                                        uint64_t *blockno)
{
	assert(!(off % sizeof(struct bpfs_inode)));
	assert(commit == COMMIT_FREE || commit == COMMIT_ATOMIC);

	for (; off + sizeof(struct bpfs_inode) <= size; off += sizeof(struct bpfs_inode))
	{
		struct bpfs_inode *inode = (struct bpfs_inode*) (block + off);
		inode->nlinks = 0;
	}
	return 0;
}

static void reset_inodes_nlinks(void)
{
	xcall(crawl_inodes(0, get_inode_root()->nbytes, COMMIT_FREE,
	                   callback_reset_inodes_nlinks, NULL));
}

static int init_allocations(bool mounting)
{
	uint64_t i;

	xcall(init_block_allocations());
	xcall(init_inode_allocations());

	static_assert(BPFS_BLOCKNO_INVALID == 0);
	for (i = 1; i < BPFS_BLOCKNO_FIRST_ALLOC; i++)
		set_block(i);
	set_block(bpfs_super->inode_root_addr);

	discover_tree_allocations(get_inode_root());

	// Only reset inode nlinks during mounting so that we do not distrub
	// write counting
	if (mounting && !bpfs_super->ephemeral_valid)
		reset_inodes_nlinks();
	discover_inode_allocations(BPFS_INO_ROOT, mounting);
	if (mounting && !bpfs_super->ephemeral_valid)
		bpfs_super->ephemeral_valid = 1;

	return 0;
}

static void destroy_allocations(void)
{
	destroy_inode_allocations();
	destroy_block_allocations();
}

struct allocation {
	struct inode_allocation inode;
	struct block_allocation block;
};

static void stash_destroy_allocations(struct allocation *alloc)
{
	move_inode_allocations(&alloc->inode, &inode_alloc);
	move_block_allocations(&alloc->block, &block_alloc);
}

static void destroy_restore_allocations(struct allocation *alloc)
{
	move_inode_allocations(&inode_alloc, &alloc->inode);
	move_block_allocations(&block_alloc, &alloc->block);
}


//
// commit, abort, and recover

static int recover_superblock(void)
{
	struct bpfs_super *super_2 = bpfs_super + 1;

	if (bpfs_super->commit_mode != super_2->commit_mode)
		return -1;

	if (bpfs_super->commit_mode == BPFS_COMMIT_SCSP)
		return 0;

	if (bpfs_super->commit_mode != BPFS_COMMIT_SP)
		return -1;

	if (super_2->magic != BPFS_FS_MAGIC)
	{
		fprintf(stderr, "Not a BPFS file system in SP mode (incorrect magic)\n");
		return -1;
	}

	if (bpfs_super->inode_root_addr == bpfs_super->inode_root_addr_2)
	{
		if (super_2->inode_root_addr != super_2->inode_root_addr_2)
			*super_2 = *bpfs_super;
	}
	else if (super_2->inode_root_addr == super_2->inode_root_addr_2)
		*bpfs_super = *super_2;
	else
		return -2;
	return 0;
}

#if COMMIT_MODE == MODE_SP
static struct bpfs_super *persistent_super;
static struct bpfs_super staged_super;

static void revert_superblock(void)
{
	assert(bpfs_super == &staged_super);

	staged_super.inode_root_addr = persistent_super->inode_root_addr;

	assert(!memcmp(persistent_super, &staged_super, sizeof(staged_super)));
}

static void persist_superblock(void)
{
# ifndef NDEBUG
	static bool first_run = 1;
# endif
	struct bpfs_super *persistent_super_2 = persistent_super + 1;

	assert(bpfs_super == &staged_super);

	if (!memcmp(persistent_super, &staged_super, sizeof(staged_super)))
		return; // update unnecessary

# if DETECT_NONCOW_WRITES_SP
	{
		size_t len = BPFS_BLOCK_SIZE * 2; /* two super blocks */
		static_assert(BPFS_BLOCKNO_INVALID == 0
		              && BPFS_BLOCKNO_SUPER == 1 && BPFS_BLOCKNO_SUPER_2 == 2);
		xsyscall(mprotect(bpram, len, PROT_READ | PROT_WRITE));
	}
# endif

# ifndef NDEBUG
	// Only compare supers after super2 has been created
	if (first_run)
		first_run = 0;
	else
		assert(!memcmp(persistent_super, persistent_super_2,
		       sizeof(staged_super)));
# endif

	staged_super.inode_root_addr_2 = staged_super.inode_root_addr;

	// persist the inode_root_addr{,_2} fields, but do so by copying
	// all because !SCSP and to copy the ephemeral_valid field:
	memcpy(persistent_super, &staged_super, sizeof(staged_super));
	epoch_barrier(); // keep at least one SB consistent during each update
	memcpy(persistent_super_2, &staged_super, sizeof(staged_super));

# if DETECT_NONCOW_WRITES_SP
	{
		size_t len = BPFS_BLOCK_SIZE * 2; /* two super blocks */
		static_assert(BPFS_BLOCKNO_INVALID == 0
		              && BPFS_BLOCKNO_SUPER == 1 && BPFS_BLOCKNO_SUPER_2 == 2);
		xsyscall(mprotect(bpram, len, PROT_READ));
	}
# endif
}
#elif COMMIT_MODE == MODE_SCSP
static void reset_indirect_cow_superblock(void)
{
	uint64_t new_blockno;

	assert(!indirect_cow_block_get(BPFS_BLOCKNO_SUPER));
	new_blockno = cow_block_entire(BPFS_BLOCKNO_SUPER);
	xassert(new_blockno != BPFS_BLOCKNO_INVALID);

	set_super((struct bpfs_super*) get_block(new_blockno));
}

static void revert_superblock(void)
{
#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
#endif
	indirect_cow_abort();
}

static void persist_superblock(void)
{
#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(bpram, bpram_size, PROT_READ | PROT_WRITE));
#endif
	indirect_cow_commit();
#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
#endif
}
#endif

#if !DETECT_ALLOCATION_DIFFS
static void detect_allocation_diffs(void)
{
}
#else
static void print_bitmap_differences(const char *name,
                                     const char *orig_bitmap,
                                     const char *disc_bitmap,
                                     size_t size)
{
	size_t i;
	printf("%s bitmap differences (-1):", name);
	for (i = 0; i < size; i++)
	{
		bool orig = !!(orig_bitmap[i / 8] & (1 << i % 8));
		bool disc = !!(disc_bitmap[i / 8] & (1 << i % 8));
		if (orig != disc)
			printf(" %zu[%d]", i, disc);
	}
	printf("\n");
}

static void detect_allocation_diffs(void)
{
	char *orig_block_bitmap;
	char *orig_inode_bitmap;
	uint64_t orig_block_ntotal;
	uint64_t orig_inode_ntotal;
	bool diff = false;

	/* non-NULL would complicate destory+init+compare */
	assert(!block_alloc.bitmap.allocs);
	assert(!block_alloc.bitmap.frees);
	assert(!inode_alloc.bitmap.allocs);
	assert(!inode_alloc.bitmap.frees);

	orig_block_bitmap = malloc(block_alloc.bitmap.ntotal / 8);
	xassert(orig_block_bitmap);
	memcpy(orig_block_bitmap, block_alloc.bitmap.bitmap,
	       block_alloc.bitmap.ntotal / 8);
	orig_block_ntotal = block_alloc.bitmap.ntotal;

	orig_inode_bitmap = malloc(inode_alloc.bitmap.ntotal / 8);
	xassert(orig_inode_bitmap);
	memcpy(orig_inode_bitmap, inode_alloc.bitmap.bitmap,
	       inode_alloc.bitmap.ntotal / 8);
	orig_inode_ntotal = inode_alloc.bitmap.ntotal;

	destroy_allocations();
	init_allocations(false);

	assert(orig_block_ntotal == block_alloc.bitmap.ntotal);
	if (memcmp(orig_block_bitmap, block_alloc.bitmap.bitmap,
	           block_alloc.bitmap.ntotal / 8))
	{
		diff = true;
		print_bitmap_differences("block",
		                         orig_block_bitmap, block_alloc.bitmap.bitmap,
		                         block_alloc.bitmap.ntotal);
	}

	assert(orig_inode_ntotal == inode_alloc.bitmap.ntotal);
	if (memcmp(orig_inode_bitmap, inode_alloc.bitmap.bitmap,
	           inode_alloc.bitmap.ntotal / 8))
	{
		diff = true;
		print_bitmap_differences("inodes",
		                         orig_inode_bitmap, inode_alloc.bitmap.bitmap,
		                         inode_alloc.bitmap.ntotal);
	}

	assert(!diff);

	free(orig_block_bitmap);
	free(orig_inode_bitmap);
}
#endif

static void bpfs_abort(void)
{
#if COMMIT_MODE != MODE_BPFS
	revert_superblock();
#endif

	abort_blocks();
	abort_inodes();

	detect_allocation_diffs();

#if INDIRECT_COW
	reset_indirect_cow_superblock();
#endif
}

static void bpfs_commit(void)
{
#if COMMIT_MODE != MODE_BPFS
	persist_superblock();
#endif

	commit_blocks();
	commit_inodes();

	detect_allocation_diffs();

#if COMMIT_MODE == MODE_SCSP
	reset_indirect_cow_superblock();
#endif
}


static int callback_load_directory(uint64_t blockoff, char *block,
                                   unsigned off, unsigned size, unsigned valid,
                                   uint64_t crawl_start, enum commit commit,
                                   void *p_pi_void, uint64_t *blockno)
{
	uint64_t parent_ino = *(uint64_t*) p_pi_void;
	const unsigned end = off + size;
	while (off + BPFS_DIRENT_MIN_LEN <= end)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		struct mdirent mdirent;
		int r;

		assert(!(off % BPFS_DIRENT_ALIGN));

		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			r = dcache_add_free(parent_ino, blockoff * BPFS_BLOCK_SIZE + off,
			                    BPFS_BLOCK_SIZE - off);
			if (r < 0)
				return r;
			break;
		}
		off += dirent->rec_len;
		assert(off <= BPFS_BLOCK_SIZE);
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		assert(dirent->rec_len >= BPFS_DIRENT_LEN(dirent->name_len));

		mdirent_init_dirent(&mdirent, dirent,
		                    blockoff * BPFS_BLOCK_SIZE + off - dirent->rec_len);

		r = dcache_add_dirent(parent_ino, dirent->name, &mdirent);
		if (r < 0)
			return r;
	}
	return 0;
}

static int find_dirent(uint64_t parent_ino, const char *name,
                       const struct mdirent **pmd)
{
	const struct mdirent *md;

	if (!dcache_has_dir(parent_ino))
	{
		int r;
		r = dcache_add_dir(parent_ino);
		if (r < 0)
			return r;
		r = crawl_data(parent_ino, 0, BPFS_EOF, COMMIT_NONE,
		               callback_load_directory, &parent_ino);
		if (r < 0)
		{
			dcache_rem_dir(parent_ino);
			return r;
		}
	}

	md = dcache_get_dirent(parent_ino, name);
	if (!md)
		return -ENOENT;
	if (pmd)
		*pmd = md;
	return 0;
}

static int callback_get_dirent(uint64_t blockoff, char *block,
                               unsigned off, unsigned size, unsigned valid,
                               uint64_t crawl_start, enum commit commit,
                               void *dirent_void, uint64_t *blockno)
{
	struct bpfs_dirent **dirent = (struct bpfs_dirent**) dirent_void;
	*dirent = (struct bpfs_dirent*) (block + off);
	assert(off + (*dirent)->rec_len <= valid);
	return 0;
}

static struct bpfs_dirent* get_dirent(uint64_t parent_ino, uint64_t dirent_off)
{
	struct bpfs_dirent *dirent;
	int r;

	assert(get_inode(parent_ino));
	assert(get_inode(parent_ino)->nlinks);
	assert(dirent_off + BPFS_DIRENT_MIN_LEN <= get_inode(parent_ino)->root.nbytes);

	r = crawl_data(parent_ino, dirent_off, 1, COMMIT_NONE,
	               callback_get_dirent, &dirent);
	if (r < 0)
		return NULL;
	return dirent;
}

struct sd_ino
{
	uint64_t parent_ino;
	struct str_dirent *sd;
};

static int callback_dirent_plug(uint64_t blockoff, char *block,
                                unsigned off, unsigned size,  unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *si_void, uint64_t *blockno)
{
	struct sd_ino *si = (struct sd_ino*) si_void;
	struct str_dirent *sd = si->sd;
	struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
	const uint64_t min_hole_size = BPFS_DIRENT_LEN(sd->str.len);
#ifndef NDEBUG
	const unsigned end = off + size;
#endif

	assert(commit != COMMIT_NONE);
	assert(!(off % BPFS_DIRENT_ALIGN));
	assert(off + min_hole_size <= end);
	assert(!dirent->rec_len || dirent->rec_len >= min_hole_size);
	assert(off + dirent->rec_len <= BPFS_BLOCK_SIZE);
	assert(BPFS_BLOCK_SIZE - off >= min_hole_size);
	// Not necessarily true for a dirent's first use:
	// assert(dirent->ino == BPFS_INO_INVALID);

	if (commit == COMMIT_COPY)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		assert(COMMIT_COPY == COMMIT_ATOMIC);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
		dirent = (struct bpfs_dirent*) (block + off);
		*blockno = new_blockno;
	}

	// TODO: set file_type here
	if (!dirent->rec_len)
	{
		if (off + min_hole_size + BPFS_DIRENT_MIN_LEN <= BPFS_BLOCK_SIZE)
		{
			unsigned next_off = off + min_hole_size;
			struct bpfs_dirent *next_dirent
				= (struct bpfs_dirent*) (block + next_off);
			int r;
			next_dirent->rec_len = 0;
			r = dcache_add_free(si->parent_ino,
			                    blockoff * BPFS_BLOCK_SIZE + next_off,
			                    BPFS_BLOCK_SIZE - next_off);
			xassert(!r); // FIXME: recover from OOM
		}
		dirent->rec_len = min_hole_size;
	}
	dirent->name_len = sd->str.len;
	memcpy(dirent->name, sd->str.str, sd->str.len);
	sd->dirent_off = blockoff * BPFS_BLOCK_SIZE + off;
	sd->dirent = dirent;
	return 1;
}

static int callback_dirent_append(uint64_t blockoff, char *block,
                                  unsigned off, unsigned size,  unsigned valid,
                                  uint64_t crawl_start, enum commit commit,
                                  void *si_void, uint64_t *blockno)
{
	struct sd_ino *si = (struct sd_ino*) si_void;
	struct str_dirent *sd = si->sd;
	uint64_t hole_size = BPFS_DIRENT_LEN(sd->str.len);

	assert(!off && size == BPFS_BLOCK_SIZE);
	assert(crawl_start == blockoff * BPFS_BLOCK_SIZE);
	assert(!valid);
	assert(commit != COMMIT_NONE);
	assert(commit == COMMIT_FREE);

	sd->dirent_off = blockoff * BPFS_BLOCK_SIZE;
	sd->dirent = (struct bpfs_dirent*) block;

	if (hole_size + BPFS_DIRENT_MIN_LEN <= BPFS_BLOCK_SIZE)
	{
		struct bpfs_dirent *next_dirent = (struct bpfs_dirent*) (block + hole_size);
		int r;
		next_dirent->rec_len = 0;
		r = dcache_add_free(si->parent_ino,
		                    blockoff * BPFS_BLOCK_SIZE + hole_size,
		                    BPFS_BLOCK_SIZE - hole_size);
		if (r < 0)
			return r;
	}
	sd->dirent->rec_len = hole_size;

	sd->dirent->name_len = sd->str.len;
	memcpy(sd->dirent->name, sd->str.str, sd->str.len);
	// TODO: set file_type here

	return 0;
}

static int alloc_dirent(uint64_t parent_ino, struct str_dirent *sd)
{
	struct sd_ino si = { .parent_ino = parent_ino, .sd = sd };
	uint64_t off;
	int r;

	off = dcache_take_free(parent_ino, BPFS_DIRENT_LEN(sd->str.len));
	if (off != DCACHE_FREE_NONE)
	{
		r = crawl_data(parent_ino, off,
		               BPFS_BLOCK_SIZE - (off % BPFS_BLOCK_SIZE),
		               COMMIT_ATOMIC, callback_dirent_plug, &si);
		if (r < 0)
		{
			// FIXME: need to dcache_add_free(parent_ino, off, ?);
			xassert(0);
			return r;
		}
		assert(r == 1);
	}
	else
	{
		r = crawl_data(parent_ino, BPFS_EOF, BPFS_BLOCK_SIZE,
		               COMMIT_ATOMIC, callback_dirent_append, &si);
		if (r < 0)
			return r;
	}
	assert(sd->dirent_off != BPFS_EOF && sd->dirent);

	// Caller sets dirent->ino and dirent->file_type
	return 0;
}

static int callback_set_dirent_ino(uint64_t blockoff, char *block,
                                   unsigned off, unsigned size, unsigned valid,
                                   uint64_t crawl_start, enum commit commit,
                                   void *ino_void, uint64_t *blockno)
{
	uint64_t ino = *(uint64_t*) ino_void;
	struct bpfs_dirent *dirent;

	assert(commit != COMMIT_NONE);
	assert(!(off % BPFS_DIRENT_ALIGN));

	if (commit == COMMIT_COPY)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
		*blockno = new_blockno;
	}
	dirent = (struct bpfs_dirent*) (block + off);

	dirent->ino = ino;

	return 0;
}

static int callback_clear_dirent_ino(uint64_t blockoff, char *block,
                                     unsigned off, unsigned size, unsigned valid,
                                     uint64_t crawl_start, enum commit commit,
                                     void *ino_void, uint64_t *blockno)
{
	uint64_t *ino = (uint64_t*) ino_void;
	struct bpfs_dirent *dirent;

	assert(commit != COMMIT_NONE);
	assert(!(off % BPFS_DIRENT_ALIGN));

	if (commit == COMMIT_COPY)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
		*blockno = new_blockno;
	}
	dirent = (struct bpfs_dirent*) (block + off);

	*ino = dirent->ino;
	dirent->ino = BPFS_INO_INVALID;

	return 0;
}

struct callback_addrem_dirent_data {
	bool add; /* true for add, false for remove */
	uint64_t dirent_off;
	uint64_t ino; /* input for add, output for rem */
	bool dir;
};

static int callback_addrem_dirent(char *block, unsigned off,
                                  struct bpfs_inode *inode, enum commit commit,
                                  void *cadd_void, uint64_t *blockno)
{
	struct callback_addrem_dirent_data *cadd = (struct callback_addrem_dirent_data*) cadd_void;
	uint64_t new_blockno = *blockno;
	crawl_callback dirent_callback;
	int r;

	assert(commit != COMMIT_NONE);

	if (cadd->dir && cadd->add && !(inode->nlinks + 1))
		return -EMLINK;

	// TODO: why does this write 4 fewer bytes for SCSP than when it is
	// placed after crawl_tree() and the cow also guarded by same block?
	if (cadd->dir)
	{
		if (commit == COMMIT_COPY)
		{
			new_blockno = cow_block_entire(*blockno);
			if (new_blockno == BPFS_BLOCKNO_INVALID)
				return -ENOSPC;
			indirect_cow_block_required(new_blockno);
			block = get_block(new_blockno);
		}
		inode = (struct bpfs_inode*) (block + off);

		if (cadd->add)
			inode->nlinks++;
		else
			inode->nlinks--;
		assert(inode->nlinks >= 2);
	}

	dirent_callback = cadd->add ? callback_set_dirent_ino : callback_clear_dirent_ino;
	r = crawl_tree(&inode->root, cadd->dirent_off, 1,
	               commit, dirent_callback, &cadd->ino, &new_blockno);
	if (r < 0)
		return r;
#if COMMIT_MODE == MODE_BPFS
	assert(*blockno == new_blockno);
#endif

	*blockno = new_blockno;
	return 0;
}

struct callback_init_inode_data {
	mode_t mode;
	const struct fuse_ctx *ctx;
};

static int callback_init_inode(char *block, unsigned off,
                               struct bpfs_inode *inode, enum commit commit,
                               void *ciid_void, uint64_t *blockno)
{
	struct callback_init_inode_data *ciid = (struct callback_init_inode_data*) ciid_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->generation++;
	assert(inode->generation); // not allowed to repeat within a fuse session
	inode->mode = f2b_mode(ciid->mode);
	inode->uid = ciid->ctx->uid;
	inode->gid = ciid->ctx->gid;
#if DETECT_ZEROLINKS_WITH_LINKS
	assert(!inode->nlinks);
#endif
	// inode->nlinks = 1; // set by caller
	inode->flags = 0;
	// ha_set(&inode->root.ha, 0, BPFS_BLOCKNO_INVALID); // set by caller
	// inode->root.nbytes = 0; // set by caller
	inode->mtime = inode->ctime = inode->atime = BPFS_TIME_NOW();

	// NOTE: inode->pad is uninitialized.
	// A format ugprade can zero needed fields before bumping the version.
	//memset(inode->pad, 0, sizeof(inode->pad));

	*blockno = new_blockno;
	return 0;
}

static int callback_set_cmtime(char *block, unsigned off,
                               struct bpfs_inode *inode, enum commit commit,
                               void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

#if !SCSP_OPT_TIME
	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
#endif
	inode = (struct bpfs_inode*) (block + off);

	inode->ctime = inode->mtime = *new_time;
#if SCSP_OPT_TIME
	indirect_cow_block_direct(new_blockno, block_offset(&inode->mtime),
	                          sizeof(inode->mtime));
	indirect_cow_block_direct(new_blockno, block_offset(&inode->ctime),
	                          sizeof(inode->ctime));
#endif

	*blockno = new_blockno;
	return 0;
}

static int create_file(fuse_req_t req, fuse_ino_t parent_ino,
                       const char *name, mode_t mode, const char *link,
                       struct bpfs_dirent **pdirent)
{
	uint64_t ino;
	size_t name_len = strlen(name) + 1;
	struct str_dirent sd = {{name, name_len}, BPFS_EOF, NULL};
	struct callback_init_inode_data ciid = {mode, fuse_req_ctx(req)};
	struct callback_addrem_dirent_data cadd = {true, 0, BPFS_INO_INVALID, S_ISDIR(mode)};
	struct bpfs_inode *inode;
	struct mdirent mdirent;
	int r;

	assert(!!link == !!S_ISLNK(mode));

	if (name_len > BPFS_DIRENT_MAX_NAME_LEN)
		return -ENAMETOOLONG;

	if (!get_inode(parent_ino))
		return -ENOENT;
	assert(get_inode(parent_ino)->nlinks >= 2);
	assert(BPFS_S_ISDIR(get_inode(parent_ino)->mode));

	if (!find_dirent(parent_ino, name, NULL))
		return -EEXIST;

	// TODO: should we init inodes in alloc and below?
	ino = alloc_inode();
	if (ino == BPFS_INO_INVALID)
		return -ENOSPC;

	if ((r = alloc_dirent(parent_ino, &sd)) < 0)
		return r;

	r = crawl_inode(ino, COMMIT_ATOMIC, callback_init_inode, &ciid);
	if (r < 0)
		return r;

	r = crawl_inode(parent_ino, COMMIT_ATOMIC,
	                callback_set_cmtime, &get_inode(ino)->mtime);
	if (r < 0)
		return r;

	// inode's block freshly allocated for [SC]SP and inode ignored for BPFS
	inode = get_inode(ino);
	assert(inode);

	if (S_ISDIR(mode) || S_ISLNK(mode))
	{
		ha_set(&inode->root.ha, 0, alloc_block());
		if (inode->root.ha.addr == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;

		if (S_ISDIR(mode))
		{
			struct bpfs_dirent *ndirent;

			inode->nlinks = 2; // for the ".." dirent

			inode->root.nbytes = BPFS_BLOCK_SIZE;

			ndirent = (struct bpfs_dirent*) get_block(inode->root.ha.addr);
			assert(ndirent);
			ndirent->rec_len = 0;
			// this new directory is not yet in the dcache;
			// no need to add this free dirent to the dcache
		}
		else if (S_ISLNK(mode))
		{
			inode->nlinks = 1;

			inode->root.nbytes = strlen(link) + 1;

			assert(inode->root.nbytes <= BPFS_BLOCK_SIZE); // else use crawler
			memcpy(get_block(inode->root.ha.addr), link, inode->root.nbytes);
		}
	}
	else
	{
		inode->nlinks = 1;
		inode->root.nbytes = 0;
		ha_set(&inode->root.ha, 0, BPFS_BLOCKNO_INVALID);
	}

	// dirent's block is freshly allocated or already copied
	sd.dirent->file_type = f2b_filetype(mode);

	// Set sd.dirent->ino and, if S_ISDIR, increment parent->nlinks.
	cadd.dirent_off = sd.dirent_off;
	cadd.ino = ino;
	r = crawl_inode(parent_ino, COMMIT_ATOMIC, callback_addrem_dirent, &cadd);
	if (r < 0)
		return r;

	sd.dirent = get_dirent(parent_ino, sd.dirent_off);
	assert(sd.dirent);

	mdirent_init_dirent(&mdirent, sd.dirent, sd.dirent_off);
	r = dcache_add_dirent(parent_ino, name, &mdirent);
	xassert(!r); // FIXME: recover from OOM

	*pdirent = sd.dirent;
	return 0;
}


//
// fuse interface

static void fuse_init(void *userdata, struct fuse_conn_info *conn)
{
	const char *mode;
	static_assert(FUSE_ROOT_ID == BPFS_INO_ROOT);
	Dprintf("%s()\n", __FUNCTION__);
	switch (COMMIT_MODE)
	{
		case MODE_SP:   mode = "SP";   break;
		case MODE_SCSP: mode = "SCSP"; break;
		case MODE_BPFS: mode = "BPFS"; break;
		default: assert(0); mode = NULL;
	}
	printf("BPFS running in %s mode", mode);
#if SCSP_OPT_APPEND
	printf(" (SCSP_OPT_APPEND)");
#endif
#if SCSP_OPT_TIME
	printf(" (SCSP_OPT_TIME)");
#endif
	printf("\n");
	fflush(stdout);
	bpfs_commit();
}

static void fuse_destroy(void *userdata)
{
	Dprintf("%s()\n", __FUNCTION__);

	if (!bpfs_super->ephemeral_valid)
		bpfs_super->ephemeral_valid = 1;

	bpfs_commit();
}


static void fuse_statfs(fuse_req_t req, fuse_ino_t ino)
{
	struct bpfs_inode *inode = get_inode(ino);
	struct statvfs stv;
	UNUSED(ino);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	if (!inode)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	memset(&stv, 0, sizeof(stv));
	stv.f_bsize = BPFS_BLOCK_SIZE;
	stv.f_frsize = BPFS_BLOCK_SIZE;
	static_assert(sizeof(stv.f_blocks) >= sizeof(bpfs_super->nblocks));
	stv.f_blocks = bpfs_super->nblocks;
	stv.f_bfree = block_alloc.bitmap.nfree;
	stv.f_bavail = stv.f_bfree; // NOTE: no space reserved for root
	stv.f_files = inode_alloc.bitmap.ntotal - inode_alloc.bitmap.nfree;
	stv.f_ffree = inode_alloc.bitmap.nfree;
	stv.f_favail = stv.f_ffree; // NOTE: no space reserved for root
	memset(&stv.f_fsid, 0, sizeof(stv.f_fsid)); // TODO: good enough?
	stv.f_flag = 0; // TODO: check for flags (see mount(8))
	stv.f_namemax = BPFS_DIRENT_MAX_NAME_LEN - 1;

	bpfs_commit();
	xcall(fuse_reply_statfs(req, &stv));
}

static void fill_fuse_entry(const struct bpfs_dirent *dirent, struct fuse_entry_param *e)
{
	assert(get_inode(dirent->ino)->nlinks);

	memset(e, 0, sizeof(e));
	e->ino = dirent->ino;
	e->generation = get_inode(dirent->ino)->generation;
	e->attr_timeout = STDTIMEOUT;
	e->entry_timeout = STDTIMEOUT;
	xcall(bpfs_stat(e->ino, &e->attr));
}

static void mfill_fuse_entry(const struct mdirent *mdirent, struct fuse_entry_param *e)
{
	memset(e, 0, sizeof(e));
	e->ino = mdirent->ino;
	e->generation = mdirent->ino_generation;
	e->attr_timeout = STDTIMEOUT;
	e->entry_timeout = STDTIMEOUT;
	xcall(bpfs_stat(e->ino, &e->attr));
}

static void fuse_lookup(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	const struct mdirent *mdirent;
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, name, &mdirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	mfill_fuse_entry(mdirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
}

#if 0
static void fuse_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
	Dprintf("%s(ino = %lu, nlookup = %lu)\n", __FUNCTION__, ino, nlookup);

	bpfs_commit();
	fuse_reply_none(req);
}
#endif

// Not implemented: bpfs_access() (use default_permissions instead)

static void fuse_getattr(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	struct stat stbuf;
	UNUSED(fi);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	bpfs_stat(ino, &stbuf);
	bpfs_commit();
	xcall(fuse_reply_attr(req, &stbuf, STDTIMEOUT));
}

static int truncate_block_zero_leaf(uint64_t prev_blockno, uint64_t begin,
                                    uint64_t end, uint64_t valid,
                                    uint64_t *new_blockno)
{
	uint64_t blockno = prev_blockno;
	char *block;

	assert(valid <= begin);
	assert(begin < end);
	assert(end <= BPFS_BLOCK_SIZE);

#if COMMIT_MODE != MODE_BPFS
	blockno = cow_block(blockno, begin, end - begin, begin);
	if (blockno == BPFS_BLOCKNO_INVALID)
		return -ENOSPC;
	indirect_cow_block_required(blockno);
#endif
	block = get_block(blockno);

	memset(block + begin, 0, end - begin);

	*new_blockno = blockno;
	return 0;
}

static int truncate_block_zero_indir(uint64_t prev_blockno, uint64_t begin,
                                     uint64_t end, uint64_t valid,
                                     unsigned height, uint64_t max_nblocks,
                                     uint64_t *new_blockno)
{
	uint64_t blockno = prev_blockno;
	struct bpfs_indir_block *indir = (struct bpfs_indir_block*) get_block(blockno);
	uint64_t child_max_nblocks = max_nblocks / BPFS_BLOCKNOS_PER_INDIR;
	uint64_t child_max_nbytes = BPFS_BLOCK_SIZE * child_max_nblocks;
	uint64_t validno = (valid + child_max_nbytes - 1) / child_max_nbytes;
	uint64_t beginno = begin / child_max_nbytes;
	uint64_t endno = (end + child_max_nbytes - 1) / child_max_nbytes;
	bool begin_aligned = !(begin % child_max_nbytes);
	uint64_t no;

	assert(valid <= begin);
	assert(begin < end);
	assert(end <= BPFS_BLOCK_SIZE * max_nblocks);

#if COMMIT_MODE != MODE_BPFS
	{
		unsigned indir_valid = beginno * sizeof(indir->addr[0]);
		if (!begin_aligned)
			indir_valid += sizeof(indir->addr[0]);
		blockno = cow_block(blockno, 0, 0, indir_valid);
		if (blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(blockno);
		indir = (struct bpfs_indir_block*) get_block(blockno);
	}
#endif

	for (no = beginno + 1; no < endno; no++)
	{
#if APPEASE_VALGRIND
		indir->addr[no] = BPFS_BLOCKNO_INVALID;
#else
		if (indir->addr[no] != BPFS_BLOCKNO_INVALID)
			indir->addr[no] = BPFS_BLOCKNO_INVALID;
#endif
	}

	if (begin_aligned)
		indir->addr[beginno] = BPFS_BLOCKNO_INVALID;
	else if (indir->addr[beginno] != BPFS_BLOCKNO_INVALID)
	{
		uint64_t child_begin = begin - beginno * child_max_nbytes;
		uint64_t child_end = MIN(end - beginno * child_max_nbytes,
		                         child_max_nbytes);
		uint64_t child_valid;
		uint64_t child_blockno = indir->addr[beginno];
		int r;

		if (beginno + 1 == validno)
			child_valid = MIN(valid - beginno * child_max_nbytes,
			                  child_max_nbytes);
		else
		{
			assert(validno < beginno + 1);
			child_valid = 0;
		}

		xcall(indirect_cow_parent_push(blockno));
		if (height > 1)
			r = truncate_block_zero_indir(child_blockno, child_begin,
			                              child_end, child_valid, height - 1,
			                              child_max_nblocks, &child_blockno);
		else
			r = truncate_block_zero_leaf(child_blockno, child_begin,
			                             child_end, child_valid,
			                             &child_blockno);
		indirect_cow_parent_pop(blockno);
		if (r < 0)
			return r;

		if (indir->addr[beginno] != child_blockno)
			indir->addr[beginno] = child_blockno;
	}

	*new_blockno = blockno;
	return 0;
}

// Extend root to end bytes, filling root->nbytes through end with zeros
int truncate_block_zero(struct bpfs_tree_root *root,
                        uint64_t begin, uint64_t end, uint64_t valid,
                        uint64_t *blockno)
{
	uint64_t new_blockno = *blockno;
#if COMMIT_MODE != MODE_BPFS
	unsigned root_off = block_offset(root);
#endif
	uint64_t max_nblocks = tree_max_nblocks(tree_root_height(root));
	uint64_t max_nbytes = max_nblocks * BPFS_BLOCK_SIZE;
	uint64_t child_blockno = tree_root_addr(root);

	/* convenience. NOTE: EOF is a quasi end of file here. */
	if (max_nbytes <= root->nbytes)
		return 0;
	if (end == BPFS_EOF)
		end = max_nbytes;
	if (valid == BPFS_EOF)
		valid = MIN(root->nbytes, max_nbytes);

	assert(valid <= begin); // not supposed to overwrite data
	assert(begin < end);

	end = MIN(end, max_nblocks * BPFS_BLOCK_SIZE);
	if (end < begin)
		return 0;


	if (tree_root_addr(root) == BPFS_BLOCKNO_INVALID)
		return 0;

#if COMMIT_MODE != MODE_BPFS
	new_blockno = cow_block_entire(*blockno);
	if (new_blockno == BPFS_BLOCKNO_INVALID)
		return -ENOSPC;
	indirect_cow_block_required(new_blockno);
	root = (struct bpfs_tree_root*) (get_block(new_blockno) + root_off);
#endif

	if (!begin)
		child_blockno = BPFS_BLOCKNO_INVALID;
	else
	{
		int r;
		xcall(indirect_cow_parent_push(new_blockno));
		if (!tree_root_height(root))
			r = truncate_block_zero_leaf(child_blockno, begin, end, valid,
			                             &child_blockno);
		else
			r = truncate_block_zero_indir(child_blockno, begin, end, valid,
			                              tree_root_height(root), max_nblocks,
			                              &child_blockno);
		indirect_cow_parent_pop(new_blockno);
		if (r < 0)
			return r;
	}
	if (tree_root_addr(root) != child_blockno)
		ha_set_addr(&root->ha, child_blockno);

	*blockno = new_blockno;
	return 0;
}

static unsigned count_bits(unsigned x)
{
	unsigned n = 0;
	unsigned i;
	for (i = 0; i < sizeof(x) * 8; i++)
		n += !!(x & (1 << i));
	return n;
}

struct callback_setattr_data {
	struct stat *attr;
	int to_set;
};

static int callback_setattr(char *block, unsigned off,
                            struct bpfs_inode *inode, enum commit commit,
                            void *csd_void, uint64_t *blockno)
{
	struct callback_setattr_data *csd = csd_void;
	struct stat *attr = csd->attr;
	int to_set = csd->to_set;
	int nonatomic
		=   FUSE_SET_ATTR_ATIME
		  | FUSE_SET_ATTR_MTIME
#ifdef FUSE_SET_ATTR_ATIME_NOW
		  | FUSE_SET_ATTR_ATIME_NOW
		  | FUSE_SET_ATTR_MTIME_NOW
#endif
		  ;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

	if (!(commit == COMMIT_FREE
	      || (SCSP_OPT_TIME && count_bits(to_set & ~nonatomic) <= 1)
	      || (COMMIT_MODE == MODE_BPFS
	          && commit == COMMIT_ATOMIC
	          && (count_bits(to_set & ~nonatomic) <= 1
	              || to_set == (FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID)))))
	{
		new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	if (to_set & FUSE_SET_ATTR_MODE)
		inode->mode = f2b_mode(attr->st_mode);
	if ((to_set & FUSE_SET_ATTR_UID) && (to_set & FUSE_SET_ATTR_GID))
	{
		struct bpfs_inode stage = {.uid = attr->st_uid, .gid = attr->st_gid};

		static_assert(!(offsetof(struct bpfs_inode, uid) % 8));
		static_assert(offsetof(struct bpfs_inode, uid) + sizeof(inode->uid)
		              == offsetof(struct bpfs_inode, gid));
		static_assert(sizeof(inode->uid) + sizeof(inode->gid) == 8);

		memcpy(&inode->uid, &stage.uid, sizeof(inode->uid)+sizeof(inode->gid));
	}
	else if (to_set & FUSE_SET_ATTR_UID)
		inode->uid = attr->st_uid;
	else if (to_set & FUSE_SET_ATTR_GID)
		inode->gid = attr->st_gid;
	if (to_set & FUSE_SET_ATTR_SIZE && attr->st_size != inode->root.nbytes)
	{
		uint64_t new_blockno2 = new_blockno;
		int r;

		if (attr->st_size < inode->root.nbytes)
		{
			if (NBLOCKS_FOR_NBYTES(attr->st_size) < NBLOCKS_FOR_NBYTES(inode->root.nbytes))
			{
				truncate_block_free(&inode->root, attr->st_size);

				inode->root.nbytes = attr->st_size;

				r = tree_change_height(&inode->root,
				                       tree_height(NBLOCKS_FOR_NBYTES(attr->st_size)),
				                       COMMIT_ATOMIC, &new_blockno2);
				if (r < 0)
					return r;
				assert(new_blockno == new_blockno2);
			}
		}
		else
		{
			r = truncate_block_zero(&inode->root, inode->root.nbytes,
			                        BPFS_EOF, BPFS_EOF, &new_blockno2);
			if (r < 0)
				return r;
			assert(new_blockno == new_blockno2);

			inode->root.nbytes = attr->st_size;
		}
	}
	// NOTE: if add sub-second, check for FUSE_SET_ATTR_ATIME_NOW
	if (to_set & FUSE_SET_ATTR_ATIME)
		inode->atime.sec = attr->st_atime;
	// NOTE: if add sub-second, check for FUSE_SET_ATTR_MTIME_NOW
	if (to_set & FUSE_SET_ATTR_MTIME)
		inode->mtime.sec = attr->st_mtime;
	inode->ctime = BPFS_TIME_NOW();

	if (to_set & FUSE_SET_ATTR_SIZE)
		inode->mtime = inode->ctime;

#if SCSP_OPT_TIME
	if (to_set & (FUSE_SET_ATTR_ATIME))
		indirect_cow_block_direct(new_blockno, block_offset(&inode->atime),
		                          sizeof(inode->atime));
	if (to_set & (FUSE_SET_ATTR_MTIME | FUSE_SET_ATTR_SIZE))
		indirect_cow_block_direct(new_blockno, block_offset(&inode->mtime),
		                          sizeof(inode->mtime));
	indirect_cow_block_direct(new_blockno, block_offset(&inode->ctime),
	                          sizeof(inode->ctime));
#endif

	*blockno = new_blockno;
	return 0;
}

static void fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                         int to_set, struct fuse_file_info *fi)
{
	static const int supported
		=   FUSE_SET_ATTR_MODE
		  | FUSE_SET_ATTR_UID
		  | FUSE_SET_ATTR_GID
		  | FUSE_SET_ATTR_SIZE
		  | FUSE_SET_ATTR_ATIME
		  | FUSE_SET_ATTR_MTIME
#ifdef FUSE_SET_ATTR_ATIME_NOW
		  | FUSE_SET_ATTR_ATIME_NOW
		  | FUSE_SET_ATTR_MTIME_NOW
#endif
		  ;
	struct callback_setattr_data csd = {attr, to_set};
	struct stat stbuf;
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, set =", __FUNCTION__, ino);
	if (to_set & FUSE_SET_ATTR_MODE)
		Dprintf(" mode");
	if (to_set & FUSE_SET_ATTR_UID)
		Dprintf(" uid");
	if (to_set & FUSE_SET_ATTR_GID)
		Dprintf(" gid");
	if (to_set & FUSE_SET_ATTR_SIZE)
		Dprintf(" size(to %" PRId64 ")", attr->st_size);
	if (to_set & FUSE_SET_ATTR_ATIME)
	{
		Dprintf(" atime");
#ifdef FUSE_SET_ATTR_ATIME_NOW
		if (to_set & FUSE_SET_ATTR_ATIME_NOW)
			Dprintf("[now]");
#endif
	}
	if (to_set & FUSE_SET_ATTR_MTIME)
	{
		Dprintf(" mtime");
#ifdef FUSE_SET_ATTR_MTIME_NOW
		if (to_set & FUSE_SET_ATTR_MTIME_NOW)
			Dprintf("[now]");
#endif
	}
	Dprintf(")\n");

	assert(!(to_set & ~supported));
	to_set &= supported;

	assert(get_inode(ino)->nlinks);

	r = crawl_inode(ino, COMMIT_ATOMIC, callback_setattr, &csd);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	bpfs_stat(ino, &stbuf);
	bpfs_commit();
	xcall(fuse_reply_attr(req, &stbuf, STDTIMEOUT));
}

static void fuse_readlink(fuse_req_t req, fuse_ino_t ino)
{
	struct bpfs_inode *inode = get_inode(ino);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	assert(BPFS_S_ISLNK(inode->mode));
	assert(inode->root.nbytes);
	assert(inode->root.nbytes <= BPFS_BLOCK_SIZE);
	assert(inode->nlinks);

	bpfs_commit();
	xcall(fuse_reply_readlink(req, get_block(tree_root_addr(&inode->root))));
}

static void fuse_mknod(fuse_req_t req, fuse_ino_t parent_ino, const char *name,
                       mode_t mode, dev_t rdev)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	if (S_ISBLK(mode) || S_ISCHR(mode))
	{
		// Need to store rdev to support these two types
		bpfs_abort();
		fuse_reply_err(req, ENOSYS);
		return;
	}

	r = create_file(req, parent_ino, name, mode, NULL, &dirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
}

static void fuse_mkdir(fuse_req_t req, fuse_ino_t parent_ino, const char *name,
                       mode_t mode)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = create_file(req, parent_ino, name, mode | S_IFDIR, NULL, &dirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
}

static int callback_set_ctime(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

#if !SCSP_OPT_TIME
	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
#endif
	inode = (struct bpfs_inode*) (block + off);

	inode->ctime = *new_time;
#if SCSP_OPT_TIME
	indirect_cow_block_direct(new_blockno, block_offset(&inode->ctime),
	                          sizeof(inode->ctime));
#endif

	*blockno = new_blockno;
	return 0;
}

static int callback_change_nlinks(char *block, unsigned off,
                                  struct bpfs_inode *inode, enum commit commit,
                                  void *delta_void, uint64_t *blockno)
{
	int nlinks_delta = *(int*) delta_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

	if (nlinks_delta > 0 && inode->nlinks > inode->nlinks + nlinks_delta)
		return -EMLINK;

	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	assert(nlinks_delta >= 0 || inode->nlinks >= -nlinks_delta);
	inode->nlinks += nlinks_delta;

	*blockno = new_blockno;
	return 0;
}

static int do_unlink_inode(uint64_t ino, struct bpfs_time time_now)
{
	struct bpfs_inode *inode = get_inode(ino);
	int nlinks_delta = -1;
	int r;

	assert(inode);
	assert(inode->nlinks);
	if (inode->nlinks == 1 || BPFS_S_ISDIR(inode->mode))
	{
		assert(!BPFS_S_ISDIR(inode->mode) || inode->nlinks == 2);

#if DETECT_ZEROLINKS_WITH_LINKS
		if (BPFS_S_ISDIR(inode->mode))
			nlinks_delta = -2;
		r = crawl_inode(ino, COMMIT_ATOMIC, callback_change_nlinks,
		                &nlinks_delta);
		if (r < 0)
			return r;
		assert(!get_inode(ino)->nlinks);
#endif

		// This was the last dirent for this inode. Free the inode:
		truncate_block_free(&inode->root, 0);
		free_inode(ino);
		if (BPFS_S_ISDIR(inode->mode) && dcache_has_dir(ino))
			dcache_rem_dir(ino);
	}
	else
	{
		r = crawl_inode(ino, COMMIT_ATOMIC, callback_change_nlinks,
		                &nlinks_delta);
		if (r < 0)
			return r;

		r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_ctime, &time_now);
		if (r < 0)
			return r;
	}

	return 0;
}

static int do_unlink(uint64_t parent_ino, const struct mdirent *md)
{
	struct bpfs_time time_now = BPFS_TIME_NOW();
	struct callback_addrem_dirent_data cadd =
		{false, md->off, BPFS_INO_INVALID,
		 BPFS_S_ISDIR(get_inode(md->ino)->mode)};
	int r;

	assert(get_inode(md->ino)->nlinks);

	r = crawl_inode(parent_ino, COMMIT_ATOMIC, callback_addrem_dirent, &cadd);
	if (r < 0)
		return r;

	r = crawl_inode(parent_ino, COMMIT_ATOMIC, callback_set_cmtime, &time_now);
	if (r < 0)
		return r;

	r = do_unlink_inode(md->ino, time_now);
	if (r < 0)
		return r;

	r = dcache_add_free(parent_ino, md->off, md->rec_len);
	xassert(!r); // FIXME: recover from OOM

	r = dcache_rem_dirent(parent_ino, md->name);
	assert(!r);

	return 0;
}

static void fuse_unlink(fuse_req_t req, fuse_ino_t parent_ino,
                        const char *name)
{
	const struct mdirent *mdirent;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, name, &mdirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	r = do_unlink(parent_ino, mdirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
	}
	else
	{
		bpfs_commit();
		xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	}
}

#ifndef NDEBUG
static int callback_empty_dir(uint64_t blockoff, char *block,
                              unsigned off, unsigned size, unsigned valid,
                              uint64_t crawl_start, enum commit commit,
                              void *ppi, uint64_t *blockno)
{
	uint64_t parent_ino = *(fuse_ino_t*) ppi;
	unsigned end = off + size;
	while (off + BPFS_DIRENT_MIN_LEN <= end)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		assert(off <= BPFS_BLOCK_SIZE);
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		if (dirent->ino == parent_ino)
			continue;
		return 1;
	}
	return 0;
}
#endif

static void fuse_rmdir(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	const struct mdirent *mdirent;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, name, &mdirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

#ifndef NDEBUG
	r = crawl_data(mdirent->ino, 0, BPFS_EOF, COMMIT_NONE,
	               callback_empty_dir, &parent_ino);
	xassert(r >= 0);
	if (r == 1)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, ENOTEMPTY));
		return;
	}
#endif

	r = do_unlink(parent_ino, mdirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
	}
	else
	{
		bpfs_commit();
		xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	}
}

static void fuse_symlink(fuse_req_t req, const char *link,
                         fuse_ino_t parent_ino, const char *name)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(link = '%s', parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, link, parent_ino, name);

	r = create_file(req, parent_ino, name, S_IFLNK | 0777, link, &dirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
}

static void fuse_rename(fuse_req_t req,
                        fuse_ino_t src_parent_ino, const char *src_name,
                        fuse_ino_t dst_parent_ino, const char *dst_name)
{
	const struct mdirent *src_md;
	const struct mdirent *edst_md;
	struct mdirent ndst_md;
	struct bpfs_dirent *dst_dirent;
	struct bpfs_time time_now = BPFS_TIME_NOW();
	uint64_t invalid_ino = BPFS_INO_INVALID;
	uint64_t unlinked_ino = BPFS_INO_INVALID;
	uint64_t dst_off;
	bool dst_existed;
	int r;

	Dprintf("%s(src_parent_ino = %lu, src_name = '%s',"
	        " dst_parent_ino = %lu, dst_name = '%s')\n",
	        __FUNCTION__, src_parent_ino, src_name, dst_parent_ino, dst_name);

	r = find_dirent(src_parent_ino, src_name, &src_md);
	if (r < 0)
		goto abort;
	assert(get_inode(src_md->ino)->nlinks);

	r = find_dirent(dst_parent_ino, dst_name, &edst_md);
	if (r < 0 && r != -ENOENT)
		goto abort;
	dst_existed = (r != -ENOENT);

	if (dst_existed)
	{
		assert(get_inode(edst_md->ino)->nlinks);

		unlinked_ino = edst_md->ino;
		// TODO: check that types match?
		dst_off = edst_md->off;
	}
	else
	{
		struct str_dirent dst_sd = {{dst_name, strlen(dst_name) + 1},
		                            BPFS_EOF, NULL};
		r = alloc_dirent(dst_parent_ino, &dst_sd);
		if (r < 0)
			goto abort;
		dst_off = dst_sd.dirent_off;

// TODO: enable check for MODE_SCSP (need bpram_blockno() support):
#if COMMIT_MODE == MODE_SP
		assert(block_freshly_alloced(bpram_blockno(dst_sd.dirent)));
#else
		// TODO: the assignment to dst_sd.dirent assumes that it is not
		// yet referenced. Assert this (how?) or remove this assumption.
#endif
		dst_sd.dirent->file_type = src_md->file_type;
	}

	r = crawl_data_2(dst_parent_ino, dst_off, 1,
	                 callback_set_dirent_ino, (void*) &src_md->ino,
	                 src_parent_ino, src_md->off, 1,
	                 callback_set_dirent_ino, &invalid_ino,
	                 COMMIT_ATOMIC);
	if (r < 0)
		goto abort;
	assert(get_dirent(src_parent_ino, src_md->off)->ino == BPFS_INO_INVALID);
	assert(get_dirent(dst_parent_ino, dst_off)->ino == src_md->ino);

	r = crawl_inode(dst_parent_ino, COMMIT_ATOMIC, callback_set_cmtime,
	                &time_now);
	if (r < 0)
		goto abort;
	if (dst_parent_ino != src_parent_ino)
	{
		r = crawl_inode(src_parent_ino, COMMIT_ATOMIC, callback_set_cmtime,
		                &time_now);
		if (r < 0)
			goto abort;
	}

	if (src_md->file_type == BPFS_TYPE_DIR)
	{
		if (src_parent_ino != dst_parent_ino)
		{
			int nlinks_delta = -1;
			r = crawl_inode(src_parent_ino, COMMIT_ATOMIC,
			                callback_change_nlinks, &nlinks_delta);
			if (r < 0)
				goto abort;
		}

		if (src_parent_ino != dst_parent_ino && !dst_existed)
		{
			int nlinks_delta = 1;
			r = crawl_inode(dst_parent_ino, COMMIT_ATOMIC,
			                callback_change_nlinks, &nlinks_delta);
			if (r < 0)
				goto abort;
		}
		else if (src_parent_ino == dst_parent_ino && dst_existed)
		{
			int nlinks_delta = -1;
			r = crawl_inode(dst_parent_ino, COMMIT_ATOMIC,
			                callback_change_nlinks, &nlinks_delta);
			if (r < 0)
				goto abort;
		}

		// Update the child ino's ctime because rename can change its
		// ".." dirent's ino field. We would make this field update here,
		// but the ".." dirent is computed on the fly and not stored on disk.
		r = crawl_inode(src_md->ino, COMMIT_ATOMIC, callback_set_ctime,
		                &time_now);
		if (r < 0)
			goto abort;
	}

	if (unlinked_ino != BPFS_INO_INVALID)
	{
		r = do_unlink_inode(unlinked_ino, time_now);
		if (r < 0)
			goto abort;

		r = dcache_rem_dirent(dst_parent_ino, dst_name);
		assert(!r);
	}

	r = dcache_add_free(src_parent_ino, src_md->off, src_md->rec_len);
	xassert(!r); // FIXME: recover from OOM
	r = dcache_rem_dirent(src_parent_ino, src_name);
	assert(!r);

	dst_dirent = get_dirent(dst_parent_ino, dst_off);
	assert(dst_dirent);
	mdirent_init_dirent(&ndst_md, dst_dirent, dst_off);
	r = dcache_add_dirent(dst_parent_ino, dst_name, &ndst_md);
	xassert(!r); // FIXME: recover from OOM

	bpfs_commit();
	xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	return;

  abort:
	bpfs_abort();
	xcall(fuse_reply_err(req, -r));
}

static void fuse_link(fuse_req_t req, fuse_ino_t fuse_ino,
                      fuse_ino_t parent_ino, const char *name)
{
	uint64_t ino = fuse_ino; // fuse_ino_t may be a uint32_t and we use &ino
	size_t name_len = strlen(name) + 1;
	struct mdirent mdirent;
	struct str_dirent sd = {{name, name_len}, BPFS_EOF, NULL};
	int nlinks_delta = 1;
	struct bpfs_time time_now = BPFS_TIME_NOW();
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(ino = %lu, parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, fuse_ino, parent_ino, name);

	if (name_len > BPFS_DIRENT_MAX_NAME_LEN)
	{
		r = -ENAMETOOLONG;
		goto abort;
	}

	assert(get_inode(ino));
	assert(!BPFS_S_ISDIR(get_inode(ino)->mode));
	assert(get_inode(ino)->nlinks);
	if (!(get_inode(ino)->nlinks + 1))
	{
		r = -EMLINK;
		goto abort;
	}

	if (!get_inode(parent_ino))
	{
		r = -ENOENT;
		goto abort;
	}
	assert(get_inode(parent_ino)->nlinks);
	assert(BPFS_S_ISDIR(get_inode(parent_ino)->mode));

	if (!find_dirent(parent_ino, name, NULL))
	{
		r = -EEXIST;
		goto abort;
	}

	if ((r = alloc_dirent(parent_ino, &sd)) < 0)
		goto abort;

	r = crawl_inode(parent_ino, COMMIT_ATOMIC, callback_set_cmtime, &time_now);
	if (r < 0)
		goto abort;

	r = crawl_inode(ino, COMMIT_ATOMIC, callback_change_nlinks, &nlinks_delta);
	if (r < 0)
		goto abort;
	r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_ctime, &time_now);
	if (r < 0)
		goto abort;

	// dirent's block is freshly allocated or already copied
	sd.dirent->file_type = f2b_filetype(get_inode(ino)->mode);

	r = crawl_data(parent_ino, sd.dirent_off, 1, COMMIT_ATOMIC,
	               callback_set_dirent_ino, &ino);
	if (r < 0)
		goto abort;
	sd.dirent = get_dirent(parent_ino, sd.dirent_off);
	assert(sd.dirent);

	mdirent_init_dirent(&mdirent, sd.dirent, sd.dirent_off);
	r = dcache_add_dirent(parent_ino, name, &mdirent);
	xassert(!r); // FIXME: recover from OOM

	fill_fuse_entry(sd.dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
	return;

  abort:
	bpfs_abort();
	xcall(fuse_reply_err(req, -r));
}

static void fuse_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	assert(get_inode(ino)->nlinks);

	fi->fh = ino;

	bpfs_commit();
	xcall(fuse_reply_open(req, fi));
}

struct readdir_params
{
	fuse_req_t req;
	size_t max_size;
	off_t total_size;
	char *buf;
};

static int callback_readdir(uint64_t blockoff, char *block,
                            unsigned off, unsigned size, unsigned valid,
                            uint64_t crawl_start, enum commit commit,
                            void *p_void, uint64_t *blockno)
{
	struct readdir_params *params = (struct readdir_params*) p_void;
	const unsigned end = off + size;
	while (off + BPFS_DIRENT_MIN_LEN <= end)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		off_t oldsize = params->total_size;
		struct stat stbuf;
		size_t fuse_dirent_size;

		assert(!(off % BPFS_DIRENT_ALIGN));

		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		assert(off <= BPFS_BLOCK_SIZE);
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		assert(dirent->rec_len >= BPFS_DIRENT_LEN(dirent->name_len));

		memset(&stbuf, 0, sizeof(stbuf));
		stbuf.st_ino = dirent->ino;
		stbuf.st_mode = b2f_filetype(dirent->file_type);

		fuse_dirent_size = fuse_add_direntry(params->req, NULL, 0,
		                                     dirent->name, NULL, 0);
		if (params->total_size + fuse_dirent_size > params->max_size)
			return 1;
		params->total_size += fuse_dirent_size;
		params->buf = (char*) realloc(params->buf, params->total_size);
		if (!params->buf)
			return -ENOMEM; // PERHAPS: retry with a smaller max_size?

		fuse_add_direntry(params->req, params->buf + oldsize,
		                  params->total_size - oldsize, dirent->name, &stbuf,
		                  DIRENT_FIRST_PERSISTENT_OFFSET
		                  + blockoff * BPFS_BLOCK_SIZE + off);
	}
	return 0;
}

static int callback_set_atime(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

#if !SCSP_OPT_TIME
	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
#endif
	inode = (struct bpfs_inode*) (block + off);

	inode->atime = *new_time;
#if SCSP_OPT_TIME
	indirect_cow_block_direct(new_blockno, block_offset(&inode->atime),
	                          sizeof(inode->atime));
#endif

	*blockno = new_blockno;
	return 0;
}

// FIXME: is readdir() supposed to not notice changes made after the opendir?
static void fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t max_size,
                         off_t off, struct fuse_file_info *fi)
{
	uint64_t parent_ino = fi->fh;
	struct bpfs_inode *inode = get_inode(ino);
	struct readdir_params params = {req, max_size, 0, NULL};
	struct bpfs_time time_now = BPFS_TIME_NOW();
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ")\n",
	        __FUNCTION__, ino, off);

	assert(inode->nlinks);

	if (!inode)
	{
		r = -EINVAL;
		goto abort;
	}
	if (!(BPFS_S_ISDIR(inode->mode)))
	{
		r = -ENOTDIR;
		goto abort;
	}

	while (off < DIRENT_FIRST_PERSISTENT_OFFSET)
	{
		static const char* name[] = {".", ".."};
		struct stat stbuf;
		size_t fuse_dirent_size;
		int name_i = off;
		off_t oldsize = params.total_size;

		memset(&stbuf, 0, sizeof(stbuf));
		stbuf.st_ino = (off == 0) ? ino : parent_ino;
		stbuf.st_mode = S_IFDIR;
		off++;

		fuse_dirent_size = fuse_add_direntry(req, NULL, 0,
		                                     name[name_i], NULL, 0);
		// should be true:
		xassert(params.total_size + fuse_dirent_size <= params.max_size);
		params.total_size += fuse_dirent_size;
		params.buf = (char*) realloc(params.buf, params.total_size);
		if (!params.buf)
		{
			r = -ENOMEM;
			goto abort;
		}

		fuse_add_direntry(req, params.buf + oldsize,
		                  params.total_size - oldsize, name[name_i],
		                  &stbuf, off);
	}
	assert(off >= DIRENT_FIRST_PERSISTENT_OFFSET);

	r = crawl_data(ino, off - DIRENT_FIRST_PERSISTENT_OFFSET, BPFS_EOF,
	               COMMIT_NONE, callback_readdir, &params);
	if (r < 0)
		goto abort;

	r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_atime, &time_now);
	if (r < 0)
		goto abort;

	bpfs_commit();
	xcall(fuse_reply_buf(req, params.buf, params.total_size));
	free(params.buf);
	return;

  abort:
	bpfs_abort();
	xcall(fuse_reply_err(req, -r));
	free(params.buf);
}

#if 0
static void fuse_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
	bpfs_commit();
}
#endif

static int sync_inode(uint64_t ino, int datasync)
{
	// TODO: flush cache lines and memory controller
#if 0
	printf("fsync(ino = %" PRIu64 ", datasync = %d): not yet implemented\n",
	       ino, datasync);
#endif

	assert(get_inode(ino)->nlinks);

	return 0;
}

static void fuse_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                          struct fuse_file_info *fi)
{
	int r;
	Dprintf("%s(ino = %lu, datasync = %d)\n", __FUNCTION__, ino, datasync);

	r = sync_inode(ino, datasync);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
	}
	else
	{
		bpfs_commit();
		xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	}
}

static void fuse_create(fuse_req_t req, fuse_ino_t parent_ino,
                        const char *name, mode_t mode,
                        struct fuse_file_info *fi)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = create_file(req, parent_ino, name, mode, NULL, &dirent);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_create(req, &e, fi));
}

static void fuse_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
{
	struct bpfs_inode *inode;

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	inode = get_inode(ino);
	if (!inode)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	assert(inode->nlinks);
	if (BPFS_S_ISDIR(inode->mode))
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, EISDIR));
		return;
	}
	// TODO: should we detect EACCES?

	// TODO: fi->flags: O_APPEND, O_NOATIME?

	bpfs_commit();
	xcall(fuse_reply_open(req, fi));
}

static int callback_read(uint64_t blockoff, char *block,
                         unsigned off, unsigned size, unsigned valid,
                         uint64_t crawl_start, enum commit commit,
                         void *iov_void, uint64_t *new_blockno)
{
	struct iovec *iov = iov_void;
	iov += blockoff - crawl_start / BPFS_BLOCK_SIZE;
	iov->iov_base = block + off;
	iov->iov_len = size;
	return 0;
}

static void fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	struct bpfs_time time_now = BPFS_TIME_NOW();
	uint64_t first_blockoff, last_blockoff, nblocks;
	struct iovec *iov;
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ", size = %zu)\n",
	        __FUNCTION__, ino, off, size);

	if (!inode)
	{
		r = -ENOENT;
		goto abort;
	}
	assert(inode->nlinks);
	assert(BPFS_S_ISREG(inode->mode));

	if (off >= inode->root.nbytes)
	{
		bpfs_abort();
		xcall(fuse_reply_buf(req, NULL, 0));
		return;
	}

	size = MIN(size, inode->root.nbytes - off);
	first_blockoff = off / BPFS_BLOCK_SIZE;
	last_blockoff = (off + size) ? (off + size - 1) / BPFS_BLOCK_SIZE : 0;
	nblocks = last_blockoff - first_blockoff + 1;
	iov = calloc(nblocks, sizeof(*iov));
	if (!iov)
	{
		r = -ENOMEM;
		goto abort;
	}
	r = crawl_data(ino, off, size, COMMIT_NONE, callback_read, iov);
	assert(r >= 0);

	r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_atime, &time_now);
	if (r < 0)
		goto abort;

	bpfs_commit();
	xcall(fuse_reply_iov(req, iov, nblocks));
	free(iov);
	return;

  abort:
	bpfs_abort();
	xcall(fuse_reply_err(req, -r));
}

static int callback_write(uint64_t blockoff, char *block,
                          unsigned off, unsigned size, unsigned valid,
                          uint64_t crawl_start, enum commit commit,
                          void *buf, uint64_t *new_blockno)
{
	uint64_t buf_offset = blockoff * BPFS_BLOCK_SIZE + off - crawl_start;

	assert(commit != COMMIT_NONE);
	if (!(commit == COMMIT_FREE
	      || (SCSP_OPT_APPEND && off >= valid)
	      || (COMMIT_MODE == MODE_BPFS
	          && (commit == COMMIT_ATOMIC
	              && (can_atomic_write(off, size) || off >= valid)))))
	{
		uint64_t newno = cow_block(*new_blockno, off, size, valid);
		if (newno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(newno);
		*new_blockno = newno;
		block = get_block(newno);
	}

	// TODO: if can_atomic_write(), will memcpy() make just one write?
	memcpy(block + off, buf + buf_offset, size);
	if (SCSP_OPT_APPEND && off >= valid)
		indirect_cow_block_direct(*new_blockno, off, size);

	return 0;
}


static int callback_set_mtime(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

#if !SCSP_OPT_TIME
	if (commit == COMMIT_COPY)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		indirect_cow_block_required(new_blockno);
		block = get_block(new_blockno);
	}
#endif
	inode = (struct bpfs_inode*) (block + off);

	inode->mtime = *new_time;
#if SCSP_OPT_TIME
	indirect_cow_block_direct(new_blockno, block_offset(&inode->mtime),
	                          sizeof(inode->mtime));
#endif

	*blockno = new_blockno;
	return 0;
}

static void fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
{
	// cast away const for crawl_data(), since it accepts char* (but won't
	// modify). cast into a new variable to avoid spurious compile warning.
	char *buf_unconst = (char*) buf;
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ", size = %zu)\n",
	        __FUNCTION__, ino, off, size);

	assert(get_inode(ino)->nlinks);

	r = crawl_data(ino, off, size, COMMIT_ATOMIC, callback_write, buf_unconst);
	if (r >= 0)
	{
		struct bpfs_time time_now = BPFS_TIME_NOW();
		r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_mtime, &time_now);
#if COMMIT_MODE == MODE_BPFS
		assert(r >= 0);
#endif
	}

	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
	}
	else
	{
		bpfs_commit();
		xcall(fuse_reply_write(req, size));
	}
}

#if 0
static void fuse_flush(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
	bpfs_commit();
	xcall(fuse_reply_err(req, ENOSYS));
}

static void fuse_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
	bpfs_commit();
}
#endif

static void fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                       struct fuse_file_info *fi)
{
	int r;
	Dprintf("%s(ino = %lu, datasync = %d)\n", __FUNCTION__, ino, datasync);

	r = sync_inode(ino, datasync);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
	}
	else
	{
		bpfs_commit();
		xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	}
}


static void init_fuse_ops(struct fuse_lowlevel_ops *fuse_ops)
{
	memset(fuse_ops, 0, sizeof(*fuse_ops));

#define ADD_FUSE_CALLBACK(name) fuse_ops->name = fuse_##name

	ADD_FUSE_CALLBACK(init);
	ADD_FUSE_CALLBACK(destroy);

	ADD_FUSE_CALLBACK(statfs);
	ADD_FUSE_CALLBACK(lookup);
//	ADD_FUSE_CALLBACK(forget);
	ADD_FUSE_CALLBACK(getattr);
	ADD_FUSE_CALLBACK(setattr);
	ADD_FUSE_CALLBACK(readlink);
	ADD_FUSE_CALLBACK(mknod);
	ADD_FUSE_CALLBACK(mkdir);
	ADD_FUSE_CALLBACK(unlink);
	ADD_FUSE_CALLBACK(rmdir);
	ADD_FUSE_CALLBACK(symlink);
	ADD_FUSE_CALLBACK(rename);
	ADD_FUSE_CALLBACK(link);

//	ADD_FUSE_CALLBACK(setxattr);
//	ADD_FUSE_CALLBACK(getxattr);
//	ADD_FUSE_CALLBACK(listxattr);
//	ADD_FUSE_CALLBACK(removexattr);

	ADD_FUSE_CALLBACK(opendir);
	ADD_FUSE_CALLBACK(readdir);
//	ADD_FUSE_CALLBACK(releasedir);
	ADD_FUSE_CALLBACK(fsyncdir);

	ADD_FUSE_CALLBACK(create);
	ADD_FUSE_CALLBACK(open);
	ADD_FUSE_CALLBACK(read);
	ADD_FUSE_CALLBACK(write);
//	ADD_FUSE_CALLBACK(flush);
//	ADD_FUSE_CALLBACK(release);
	ADD_FUSE_CALLBACK(fsync);

//	ADD_FUSE_CALLBACK(getlk);
//	ADD_FUSE_CALLBACK(setlk);

//	ADD_FUSE_CALLBACK(bmap); // nonsensical for BPFS: no backing block device

#undef ADD_FUSE_CALLBACK
}


//
// random fsck

void random_fsck(int signo)
{
	struct allocation alloc;
	struct itimerval itv;

	stash_destroy_allocations(&alloc);
	init_allocations(false);
	destroy_restore_allocations(&alloc);
	Dprintf("fsck passed\n");

	memset(&itv, 0, sizeof(itv));
	static_assert(RFSCK_MAX_INTERVAL <= RAND_MAX);
	itv.it_value.tv_usec = rand() % RFSCK_MAX_INTERVAL;
	xsyscall(setitimer(ITIMER_VIRTUAL, &itv, NULL));
}


//
// persistent bpram

static int bpram_fd = -1;

static void init_persistent_bpram(const char *filename)
{
	struct stat stbuf;

	assert(!bpram && !bpram_size);

	bpram_fd = xsyscall(open(filename, O_RDWR));

	xsyscall(fstat(bpram_fd, &stbuf));
	bpram_size = stbuf.st_size;
	xassert(bpram_size == stbuf.st_size);

	bpram = mmap(NULL, bpram_size, PROT_READ | PROT_WRITE, MAP_SHARED, bpram_fd, 0);
	xassert(bpram);
	// some code assumes block memory address are block aligned
	xassert(!block_offset(bpram));
}

static void destroy_persistent_bpram(void)
{
	xsyscall(msync(bpram, bpram_size, MS_SYNC));
	xsyscall(munmap(bpram, bpram_size));
	bpram = NULL;
	bpram_size = 0;
	xsyscall(close(bpram_fd));
	bpram_fd = -1;
}


//
// ephemeral bpram

static void init_ephemeral_bpram(size_t size)
{
	void *bpram_void = bpram; // convert &bpram to a void** without alias warn
	int r;
	assert(!bpram && !bpram_size);
	// some code assumes block memory address are block aligned
	r = posix_memalign(&bpram_void, BPFS_BLOCK_SIZE, size);
	xassert(!r); // note: posix_memalign() returns positives on error
	bpram = bpram_void;
	bpram_size = size;
	xcall(mkbpfs(bpram, bpram_size));
}

static void destroy_ephemeral_bpram(void)
{
	free(bpram);
	bpram = NULL;
	bpram_size = 0;
}


//
// main

void inform_pin_of_bpram(const char *bpram_addr, size_t size)
	__attribute__((noinline));

void inform_pin_of_bpram(const char *bpram_addr, size_t size)
{
	// This function exists to let the Pin tool bpramcount know:
	// 1) that bpfs has the address and size of bpram
	// 2) the value of these two parameters

	// This code exists to ensure that the compiler does not optimize
	// away calls to this function. This syscall probably cannot be
	// optimized away.
	(void) getpid();
}

int main(int argc, char **argv)
{
	void (*destroy_bpram)(void);
	int fargc;
	char **fargv;
	int r = -1;

	xassert(!hash_map_init());

	if (argc < 3)
	{
		fprintf(stderr, "%s: <-f FILE|-s SIZE> [FUSE...]\n", argv[0]);
		exit(1);
	}

	if (!strcmp(argv[1], "-f"))
	{
		init_persistent_bpram(argv[2]);
		destroy_bpram = destroy_persistent_bpram;
	}
	else if (!strcmp(argv[1], "-s"))
	{
		init_ephemeral_bpram(strtol(argv[2], NULL, 0));
		destroy_bpram = destroy_ephemeral_bpram;
	}
	else
	{
		fprintf(stderr, "Invalid argument \"%s\"\n", argv[1]);
		exit(1);
	}

	set_super(get_bpram_super());

	if (bpfs_super->magic != BPFS_FS_MAGIC)
	{
		fprintf(stderr, "Not a BPFS file system (incorrect magic)\n");
		return -1;
	}
	if (bpfs_super->version != BPFS_STRUCT_VERSION)
	{
		fprintf(stderr, "File system formatted as v%u, but software is for v%u\n",
		        bpfs_super->version, BPFS_STRUCT_VERSION);
		return -1;
	}
	if (bpfs_super->nblocks * BPFS_BLOCK_SIZE < bpram_size)
	{
		fprintf(stderr, "BPRAM is smaller than the file system\n");
		return -1;
	}

	if (recover_superblock() < 0)
	{
		fprintf(stderr, "Unable to recover BPFS superblock\n");
		return -1;
	}

#if COMMIT_MODE == MODE_SP
	bpfs_super[1].commit_mode = bpfs_super->commit_mode = BPFS_COMMIT_SP;
	persistent_super = bpfs_super;
	staged_super = *bpfs_super;
	set_super(&staged_super);
#else
	bpfs_super[1].commit_mode = bpfs_super->commit_mode = BPFS_COMMIT_SCSP;
#endif

	crawler_init();

#if INDIRECT_COW
	xcall(indirect_cow_init());
#endif

	xcall(init_allocations(true));

#if COMMIT_MODE == MODE_BPFS
	// NOTE: could instead clear and set this field for each system call
	bpfs_super[1].ephemeral_valid = bpfs_super->ephemeral_valid = 0;
#endif

# if COMMIT_MODE == MODE_SCSP
	reset_indirect_cow_superblock();
# endif

	inform_pin_of_bpram(bpram, bpram_size);

#if DETECT_NONCOW_WRITES_SCSP
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
#endif

#if DETECT_STRAY_ACCESSES
	xsyscall(mprotect(bpram, bpram_size, PROT_NONE));
	{
		uint64_t blockno;
		static_assert(BPFS_BLOCKNO_INVALID == 0);
		for (blockno = 1; blockno < bpfs_super->nblocks + 1; blockno++)
			if (block_is_alloced(blockno))
				xsyscall(mprotect(bpram + (blockno - 1) * BPFS_BLOCK_SIZE,
				                  BPFS_BLOCK_SIZE, PROT_INUSE_OLD));
	}
#endif

	if (getenv("RFSCK"))
	{
#if BLOCK_POISON
		printf("Not enabling random fsck: BLOCK_POISON is enabled.\n");
#elif INDIRECT_COW
		// There are periods when bpfs_super does not agree with the bitmaps
		printf("Not enabling random fsck: INDIRECT_COW is enabled.\n");
#else
		struct itimerval itv;

		if (!strcmp(getenv("RFSCK"), ""))
			srand(time(NULL));
		else
			srand(atoi(getenv("RFSCK")));

		xsyscall(getitimer(ITIMER_VIRTUAL, &itv));
		if (itv.it_value.tv_sec || itv.it_value.tv_usec)
			printf("ITIMER_VIRTUAL already in use. Not enabling random fsck.\n");
		else
		{
			xassert(!signal(SIGVTALRM, random_fsck));
			// start the timer (and needlessly do an fsck):
			random_fsck(SIGVTALRM);
		}
#endif
	}

#if BLOCK_POISON
	printf("Block poisoning enabled. Write counting will be incorrect.\n");
#endif

	xcall(dcache_init());

	memmove(argv + 1, argv + 3, (argc - 2) * sizeof(*argv));
	argc -= 2;

#if FUSE_BIG_WRITES
	{
		const char *argv_str_end = argv[argc - 1] + strlen(argv[argc - 1]) + 1;
		size_t argv_str_len = argv_str_end - argv[0];
		const char *bigwrites = "-obig_writes";
		char *fargv_str;
		int i;

		fargc = argc + 1;
		fargv = malloc((fargc + 1) * sizeof(*fargv));
		xassert(fargv);

		fargv_str = malloc(argv_str_len + strlen(bigwrites) + 1);
		xassert(fargv_str);
		memcpy(fargv_str, argv[0], argv_str_len);
		strcpy(&fargv_str[argv_str_len], bigwrites);

		fargv[0] = fargv_str;
		for (i = 1; i < argc; i++)
			fargv[i] = fargv_str + (argv[i] - argv[0]);
		fargv[fargc - 1] = fargv_str + argv_str_len;
		fargv[fargc] = 0;
	}
#else
	fargc = argc;
	fargv = argv;
#endif

	{
		struct fuse_args fargs = FUSE_ARGS_INIT(fargc, fargv);
		struct fuse_lowlevel_ops fuse_ops;
		struct fuse_session *se;
		struct fuse_chan *ch;
		char *mountpoint;

		init_fuse_ops(&fuse_ops);

		xcall(fuse_parse_cmdline(&fargs, &mountpoint, NULL, NULL));
		xassert((ch = fuse_mount(mountpoint, &fargs)));

		se = fuse_lowlevel_new(&fargs, &fuse_ops, sizeof(fuse_ops), NULL);
		if (se)
		{
			if (fuse_set_signal_handlers(se) != -1)
			{
				fuse_session_add_chan(se, ch);

				r = fuse_session_loop(se);

				fuse_remove_signal_handlers(se);
				fuse_session_remove_chan(ch);
			}
			fuse_session_destroy(se);
		}

		fuse_unmount(mountpoint, ch);
		free(mountpoint);
		fuse_opt_free_args(&fargs);
	}

#if FUSE_BIG_WRITES
	free(fargv[0]);
	free(fargv);
#endif
	fargv = NULL;

#if COMMIT_MODE == MODE_BPFS
	printf("CoW: %" PRIu64 " bytes in %" PRIu64 " blocks\n",
	       cow_nbytes, cow_nblocks);
#else
	// MODE_SP: doesn't count superblock
	// MODE_SCSP: current implementation can un-CoW
	printf("CoW: -1 bytes in -1 blocks\n");
#endif

	dcache_destroy();
	destroy_allocations();
#if INDIRECT_COW
	indirect_cow_destroy();
#endif
	destroy_bpram();

	return r;
}
