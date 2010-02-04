#include "mkbpfs.h"
#include "bpfs_structs.h"
#include "util.h"

#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

// TODO:
// - make time higher resolution. See ext4, bits/stat.h, linux/time.h.
// - add gcc flag to warn about ignored return values?
// - blockno can mean bpram block no or file block no
// - enable writes >4096B?
// - tell valgrind about block and inode alloc and free functions
// - merge and breakup empty dirents
// - remember that atomic 64b writes will only be atomic on 64b systems?
// - add epoch barrier function? don't reuse inflight resources?
// - change memcpy() calls to only do atomic writes if size is aligned
// - change read/crawl code to return zero block for unallocated blocks?
//   (ie because of a tree height increase)
// - merge tree height and root block address
// - accounting for subdirs in dir nlinks causes CoW
// - storing ".." in a directory causes CoW

// Set to 0 to use shadow paging, 1 to use short-circuit shadow paging
#define SCSP_ENABLED 0

#define DETECT_NONCOW_WRITES (!SCSP_ENABLED && !defined(NDEBUG))

// Set to 1 to optimize away some COWs
#define COW_OPT 0

// STDTIMEOUT is not 0 because of a fuse kernel module bug.
// Miklos's 2006/06/27 email, E1FvBX0-0006PB-00@dorka.pomaz.szeredi.hu, fixes.
#define STDTIMEOUT 1.0

#define FUSE_ERR_SUCCESS 0

#define BPFS_EOF UINT64_MAX

// Max size that can be written atomically (hardcoded for unsafe 32b testing)
#define ATOMIC_SIZE 8

#define DEBUG 1
#if DEBUG
# define Dprintf(x...) fprintf(stderr, x)
#else
# define Dprintf(x...) do {} while(0)
#endif


// TODO: repharse this as you-see-everything-p?
// NOTE: this doesn't describe situations where the top block is already COWed
//       but child blocks are refed by the original top block.
enum commit {
	COMMIT_NONE,   // no writes allowed
	COMMIT_COPY,   // writes only to copies
#if COW_OPT
	COMMIT_ATOMIC, // write in-place if write is atomic; otherwise, copy
#else
	COMMIT_ATOMIC = COMMIT_COPY,
#endif
	COMMIT_FREE,   // no restrictions on writes (e.g., region is not yet refed)
};

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


//
// forward declarations

static char* get_block(uint64_t blockno);

// Return <0 for error, 0 for success, 1 for success and stop crawl
typedef int (*crawl_callback)(uint64_t blockoff, char *block,
                              unsigned off, unsigned size, unsigned valid,
                              uint64_t crawl_start, enum commit commit,
                              void *user, uint64_t *blockno);

// Return <0 for error, 0 for success, 1 for success and stop crawl
typedef int (*crawl_callback_inode)(char *block, unsigned off,
                                    struct bpfs_inode *inode,
                                    enum commit commit, void *user,
                                    uint64_t *blockno);

typedef void (*crawl_blockno_callback)(uint64_t blockno);

static int crawl_inode(uint64_t ino, enum commit commit,
                       crawl_callback_inode callback, void *user);

static int crawl_inodes(uint64_t off, uint64_t size, enum commit commit,
                        crawl_callback callback, void *user);

static void crawl_blocknos(struct bpfs_tree_root *root,
                           uint64_t off, uint64_t size,
                           crawl_blockno_callback callback);


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

static bool staged_list_freshly_alloced(struct staged_entry *head, uint64_t no)
{
	struct staged_entry *entry;
	for (entry = head; entry; entry = entry->next)
		if (no == entry->index)
			return true;
	return false;
}


struct bitmap {
	char *bitmap;
	uint64_t ntotal;
	uint64_t nfree;
	struct staged_entry *allocs;
	struct staged_entry *frees;
};

static int bitmap_init(struct bitmap *bitmap, uint64_t ntotal)
{
	size_t size = ntotal / 8;
	xassert(!(ntotal % 8)); // simplifies resize
	xassert(!(ntotal % 8 * sizeof(uintptr_t))); // makes search faster
	assert(!bitmap->bitmap);
	bitmap->bitmap = malloc(size);
	if (!bitmap->bitmap)
		return -ENOMEM;
	memset(bitmap->bitmap, 0, size);
	bitmap->nfree = bitmap->ntotal = ntotal;
	bitmap->allocs = bitmap->frees = NULL;
	return 0;
}

static void bitmap_destroy(struct bitmap *bitmap)
{
	free(bitmap->bitmap);
	bitmap->bitmap = NULL;
	staged_list_free(&bitmap->allocs);
	staged_list_free(&bitmap->frees);
}

static int bitmap_resize(struct bitmap *bitmap, uint64_t ntotal)
{
	char *new_bitmap;

	if (bitmap->ntotal == ntotal)
		return 0;

	new_bitmap = realloc(bitmap->bitmap, ntotal);
	if (!new_bitmap)
		return -ENOMEM;
	bitmap->bitmap = new_bitmap;

	if (bitmap->ntotal < ntotal)
	{
		uint64_t delta = ntotal - bitmap->ntotal;
		memset(bitmap->bitmap + bitmap->ntotal / 8, 0, delta / 8);
		bitmap->nfree += delta;
		bitmap->ntotal = ntotal;
	}
	else
	{
		// TODO
		// might want to check that no dropped items are allocated
		xassert(0);
	}

	return 0;
}

static uint64_t bitmap_alloc(struct bitmap *bitmap)
{
	uint64_t i;
	for (i = 0; i < bitmap->ntotal; i += sizeof(uintptr_t) * 8)
	{
		uintptr_t *word = (uintptr_t*) (bitmap->bitmap + i / 8);
		if (*word != UINTPTR_MAX)
		{
			int j;
			for (j = 0; j < sizeof(*word) * 8; j++)
			{
				if (!(*word & (1 << j)))
				{
					struct staged_entry *found = malloc(sizeof(*found));
					if (!found)
						return bitmap->ntotal;
					found->index = i + j;
					found->next = bitmap->allocs;
					bitmap->allocs = found;
					*word |= 1 << j;
					bitmap->nfree--;
					return found->index;
				}
			}
		}
	}
	return bitmap->ntotal;
}

static void bitmap_free(struct bitmap *bitmap, uint64_t no)
{
	struct staged_entry *staged = malloc(sizeof(*staged));
	char *word = bitmap->bitmap + no / 8;

	assert(no < bitmap->ntotal);
	assert(*word & (1 << (no % 8)));
	assert(!staged_list_freshly_alloced(bitmap->frees, no));

	xassert(staged); // TODO/FIXME: recover

	staged->index = no;
	staged->next = bitmap->frees;
	bitmap->frees = staged;
}

static void bitmap_set(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;
	assert(no < bitmap->ntotal);
	assert(!(*word & (1 << (no % 8))));
	*word |= (1 << (no % 8));
	bitmap->nfree--;
}

static void bitmap_clear(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;

	assert(no < bitmap->ntotal);
	assert(*word & (1 << (no % 8)));

	*word &= ~(1 << (no % 8));
	bitmap->nfree++;
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
}


//
// block allocation

static struct bitmap block_bitmap;

static int init_block_allocations(void)
{
	return bitmap_init(&block_bitmap, bpfs_super->nblocks);
}

static void destroy_block_allocations(void)
{
	bitmap_destroy(&block_bitmap);
}

static uint64_t alloc_block(void)
{
	uint64_t no = bitmap_alloc(&block_bitmap);
	if (no == block_bitmap.ntotal)
		return BPFS_BLOCKNO_INVALID;
	static_assert(BPFS_BLOCKNO_INVALID == 0);
#if DETECT_NONCOW_WRITES
	xsyscall(mprotect(get_block(no + 1), BPFS_BLOCK_SIZE, PROT_READ | PROT_WRITE));
#endif
	return no + 1;
}

#if !SCSP_ENABLED
static bool block_freshly_alloced(uint64_t blockno)
{
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return staged_list_freshly_alloced(block_bitmap.allocs, blockno - 1);
}
#endif

static void set_block(uint64_t blockno)
{
	assert(blockno != BPFS_BLOCKNO_INVALID);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_set(&block_bitmap, blockno - 1);
}

static void free_block(uint64_t blockno)
{
	assert(blockno != BPFS_BLOCKNO_INVALID);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	bitmap_free(&block_bitmap, blockno - 1);
#if DETECT_NONCOW_WRITES
	xsyscall(mprotect(get_block(blockno), BPFS_BLOCK_SIZE, PROT_READ));
#endif
}

#if DETECT_NONCOW_WRITES
static void protect_bpram(void)
{
	xsyscall(mprotect(bpram, bpram_size, PROT_READ));
}
#endif

static void abort_blocks(void)
{
#if DETECT_NONCOW_WRITES
	protect_bpram();
#endif
	bitmap_abort(&block_bitmap);
}

static void commit_blocks(void)
{
#if DETECT_NONCOW_WRITES
	protect_bpram();
#endif
	bitmap_commit(&block_bitmap);
}

static char* get_block(uint64_t blockno)
{
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
	return bpram + (blockno - 1) * BPFS_BLOCK_SIZE;
}


//
// block utility functions

static uint64_t cow_block(uint64_t old_blockno,
                          unsigned off, unsigned size, unsigned valid)
{
	uint64_t new_blockno;
	char *old_block;
	char *new_block;
	uint64_t end = off + size;

#if !SCSP_ENABLED
	if (block_freshly_alloced(old_blockno))
		return old_blockno;
#endif

	new_blockno = alloc_block();
	if (new_blockno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_INVALID;

	old_block = get_block(old_blockno);
	new_block = get_block(new_blockno);
	memcpy(new_block, old_block, off);
	if (off + size < valid)
		memcpy(new_block + end, old_block + end, valid - end);
	free_block(old_blockno);
	return new_blockno;
}

static uint64_t cow_block_entire(uint64_t old_blockno)
{
	uint64_t new_blockno;
	char *old_block;
	char *new_block;

#if !SCSP_ENABLED
	if (block_freshly_alloced(old_blockno))
		return old_blockno;
#endif

	new_blockno = alloc_block();
	if (new_blockno == BPFS_BLOCKNO_INVALID)
		return BPFS_BLOCKNO_INVALID;

	old_block = get_block(old_blockno);
	new_block = get_block(new_blockno);
	memcpy(new_block, old_block, BPFS_BLOCK_SIZE);
	free_block(old_blockno);
	return new_blockno;
}

static void truncate_block_free(struct bpfs_tree_root *root, uint64_t new_size)
{
	uint64_t off = ROUNDUP64(new_size, BPFS_BLOCK_SIZE);
	if (off < root->nbytes)
		crawl_blocknos(root, off, BPFS_EOF, free_block);
}


//
// inode allocation

static struct bpfs_tree_root* get_inode_root(void)
{
	return (struct bpfs_tree_root*) get_block(bpfs_super->inode_root_addr);
}

static struct bitmap inode_bitmap;

static int init_inode_allocations(void)
{
	struct bpfs_tree_root *inode_root = get_inode_root();

	// This code assumes that inodes are contiguous in the inode tree
	static_assert(!(BPFS_BLOCK_SIZE % sizeof(struct bpfs_inode)));

	return bitmap_init(&inode_bitmap, NBLOCKS_FOR_NBYTES(inode_root->nbytes)
	                                  * BPFS_INODES_PER_BLOCK);
}

static void destroy_inode_allocations(void)
{
	bitmap_destroy(&inode_bitmap);
}

static int callback_init_inodes(uint64_t blockoff, char *block,
                                unsigned off, unsigned size, unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *user, uint64_t *blockno)
{
#ifndef NDEBUG
	// init the generation field. not required, but appeases valgrind.
	assert(!(off % sizeof(struct bpfs_inode)));
	for (; off + sizeof(struct bpfs_inode) <= size; off += sizeof(struct bpfs_inode))
	{
		struct bpfs_inode *inode = (struct bpfs_inode*) (block + off);
		inode->generation = 0;
	}
#endif
	return 0;
}

static uint64_t alloc_inode(void)
{
	uint64_t no = bitmap_alloc(&inode_bitmap);
	if (no == inode_bitmap.ntotal)
	{
		struct bpfs_tree_root *inode_root = get_inode_root();
		if (crawl_inodes(inode_root->nbytes, inode_root->nbytes, COMMIT_ATOMIC,
		                 callback_init_inodes, NULL) < 0)
			return BPFS_INO_INVALID;
		xcall(bitmap_resize(&inode_bitmap,
		                    inode_root->nbytes / sizeof(struct bpfs_inode)));
		no = bitmap_alloc(&inode_bitmap);
		assert(no != inode_bitmap.ntotal);
	}
	static_assert(BPFS_INO_INVALID == 0);
	return no + 1;
}

static void set_inode(uint64_t ino)
{
	assert(ino != BPFS_INO_INVALID);
	static_assert(BPFS_INO_INVALID == 0);
	bitmap_set(&inode_bitmap, ino - 1);
}

static void free_inode(uint64_t ino)
{
	assert(ino != BPFS_INO_INVALID);
	static_assert(BPFS_INO_INVALID == 0);
	bitmap_free(&inode_bitmap, ino - 1);
}

static void abort_inodes(void)
{
	bitmap_abort(&inode_bitmap);
}

static void commit_inodes(void)
{
	bitmap_commit(&inode_bitmap);
}

static int get_inode_offset(uint64_t ino, uint64_t *poffset)
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

	if (no >= inode_bitmap.ntotal)
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
// tree functions

static uint64_t tree_max_nblocks(uint64_t height)
{
	uint64_t max_nblocks = 1;
	while (height--)
		max_nblocks *= BPFS_BLOCKNOS_PER_INDIR;
	return max_nblocks;
}

static uint64_t tree_height(uint64_t nblocks)
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

static int tree_change_height(struct bpfs_tree_root *root,
                              unsigned height_new,
                              enum commit commit, uint64_t *blockno)
{
	uint64_t root_addr_new;

	if (root->height == height_new)
		return 0;

	if (height_new > root->height)
	{
		unsigned height_delta = height_new - root->height;
		uint64_t child_blockno = root->addr;
		while (height_delta--)
		{
			uint64_t new_blockno;
			struct bpfs_indir_block *new_indir;

			if ((new_blockno = alloc_block()) == BPFS_BLOCKNO_INVALID)
				return -ENOSPC;
			new_indir = (struct bpfs_indir_block*) get_block(new_blockno);
			new_indir->addr[0] = child_blockno;
			child_blockno = new_blockno;
		}
		root_addr_new = child_blockno;
	}
	else if (height_new < root->height)
	{
		root_addr_new = root->addr;
		if (root->nbytes)
		{
			unsigned height_delta = root->height - height_new;
			while (height_delta--)
			{
				struct bpfs_indir_block *indir = (struct bpfs_indir_block*) get_block(root_addr_new);
				uint64_t tmp = indir->addr[0];
				free_block(root_addr_new);
				root_addr_new = tmp;
			}
		}
	}

	assert(commit != COMMIT_NONE);
	// TODO: merge tree root height and addr fields to permit atomic change
	if (commit == COMMIT_COPY || commit == COMMIT_ATOMIC)
	{
		unsigned root_off = ((uintptr_t) root) % BPFS_BLOCK_SIZE;
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		root = (struct bpfs_tree_root*) (get_block(new_blockno) + root_off);
		*blockno = new_blockno;
	}

	root->addr = root_addr_new;
	root->height = height_new;
	return 0;
}


static int crawl_leaf(uint64_t prev_blockno, uint64_t blockoff,
                      unsigned off, unsigned size, unsigned valid,
                      uint64_t crawl_start, enum commit commit,
					  crawl_callback callback, void *user,
					  crawl_blockno_callback bcallback,
					  uint64_t *new_blockno)
{
	uint64_t blockno = prev_blockno;
	uint64_t child_blockno;
	int r;

	assert(crawl_start / BPFS_BLOCK_SIZE <= blockoff);
	assert(off < BPFS_BLOCK_SIZE);
	assert(off + size <= BPFS_BLOCK_SIZE);
	assert(valid <= BPFS_BLOCK_SIZE);

	if (blockno == BPFS_BLOCKNO_INVALID)
	{
		assert(commit != COMMIT_NONE);
		if ((blockno = alloc_block()) == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
	}
	child_blockno = blockno;

	if (callback)
	{
		enum commit child_commit = (blockno == prev_blockno)
		                           ? commit : COMMIT_FREE;
		r = callback(blockoff, get_block(blockno), off, size, valid,
		             crawl_start, child_commit, user, &child_blockno);
		if (r >= 0 && prev_blockno != child_blockno)
			*new_blockno = child_blockno;
	}
	else
	{
		assert(blockno == prev_blockno);
		assert(bcallback);
		bcallback(child_blockno);
		r = 0;
	}
	return r;
}

static int crawl_indir(uint64_t prev_blockno, uint64_t blockoff,
                       uint64_t off, uint64_t size, uint64_t valid,
                       uint64_t crawl_start, enum commit commit,
                       unsigned height, uint64_t max_nblocks,
                       crawl_callback callback, void *user,
                       crawl_blockno_callback bcallback,
					   uint64_t *new_blockno)
{
	uint64_t blockno = prev_blockno;
	struct bpfs_indir_block *indir;
	uint64_t child_max_nblocks = max_nblocks / BPFS_BLOCKNOS_PER_INDIR;
	uint64_t child_max_nbytes = child_max_nblocks * BPFS_BLOCK_SIZE;
	uint64_t firstno = off / (BPFS_BLOCK_SIZE * child_max_nblocks);
	uint64_t lastno = (off + size - 1) / (BPFS_BLOCK_SIZE * child_max_nblocks);
	enum commit child_commit;
	uint64_t no;
	int ret = 0;

	if (commit == COMMIT_FREE)
		child_commit = COMMIT_FREE;
	else if (commit == COMMIT_ATOMIC)
		child_commit = (firstno == lastno) ? COMMIT_ATOMIC : COMMIT_COPY;
	else
		child_commit = commit;

	if (blockno == BPFS_BLOCKNO_INVALID)
	{
		assert(commit != COMMIT_NONE);
		if ((blockno = alloc_block()) == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
	}
	indir = (struct bpfs_indir_block*) get_block(blockno);

	if (bcallback && !off)
	{
		assert(commit == COMMIT_NONE);
		assert(prev_blockno == blockno);
		bcallback(blockno);
	}

	for (no = firstno; no <= lastno; no++)
	{
		uint64_t child_off, child_size, child_valid;
		uint64_t child_blockno, child_new_blockno;
		uint64_t child_blockoff;
		int r;

		if (no == firstno)
		{
			child_off = off % child_max_nbytes;
			child_blockoff = blockoff;
		}
		else
		{
			child_off = 0;
			child_blockoff = blockoff + (no - firstno) * child_max_nblocks - ((off % child_max_nbytes) / BPFS_BLOCK_SIZE);
		}
		assert(blockoff <= child_blockoff);

		if (no == lastno)
			child_size = off + size - (no * child_max_nbytes + child_off);
		else
			child_size = child_max_nbytes - child_off;
		assert(child_size <= size);
		assert(child_size <= child_max_nbytes);

		if (no * child_max_nbytes < valid)
		{
			if ((no + 1) * child_max_nbytes <= valid)
				child_valid = child_max_nbytes;
			else
				child_valid = valid % child_max_nbytes;
		}
		else
		{
			child_valid = 0;
		}

		if (!child_valid)
			child_blockno = child_new_blockno = BPFS_BLOCKNO_INVALID;
		else
			child_blockno = child_new_blockno = indir->addr[no];

		if (height == 1)
			r = crawl_leaf(child_blockno, child_blockoff,
			               child_off, child_size, child_valid,
			               crawl_start, child_commit, callback, user,
			               bcallback, &child_new_blockno);
		else
			r = crawl_indir(child_blockno, child_blockoff,
			                child_off, child_size, child_valid,
			                crawl_start, child_commit,
			                height - 1, child_max_nblocks,
			                callback, user, bcallback,
			                &child_new_blockno);
		if (r < 0)
			return r;
		if (child_blockno != child_new_blockno)
		{
			assert(commit != COMMIT_NONE);
			// TODO: opt: no need to copy if writing to invalid entries
			if (prev_blockno == blockno && (commit == COMMIT_COPY || (commit == COMMIT_ATOMIC && firstno != lastno)))
			{
				// TODO: avoid copying data that will be overwritten?
				if ((blockno = cow_block_entire(blockno)) == BPFS_BLOCKNO_INVALID)
					return -ENOSPC;
				indir = (struct bpfs_indir_block*) get_block(blockno);
			}
			indir->addr[no] = child_new_blockno;
		}
		if (r == 1)
		{
			ret = 1;
			break;
		}
	}

	if (prev_blockno != blockno)
		*new_blockno = blockno;
	return ret;
}

// Read-only crawl over the indirect and data blocks in root
static void crawl_blocknos(struct bpfs_tree_root *root,
                           uint64_t off, uint64_t size,
                           crawl_blockno_callback callback)
{
	/* convenience */
	if (off == BPFS_EOF)
		off = root->nbytes;
	if (size == BPFS_EOF)
		size = root->nbytes - off;

	if (!root->height)
	{
		assert(off + size <= BPFS_BLOCK_SIZE);
		if (!off)
			crawl_leaf(root->addr, 0, off, size, root->nbytes, off,
			           COMMIT_NONE, NULL, NULL, callback, NULL);
	}
	else
	{
		crawl_indir(root->addr, off / BPFS_BLOCK_SIZE,
		            off, size, root->nbytes, off, COMMIT_NONE,
                    root->height, tree_max_nblocks(root->height),
		            NULL, NULL, callback, NULL);
	}
}

static int crawl_tree(struct bpfs_tree_root *root, uint64_t off, uint64_t size,
                      enum commit commit, crawl_callback callback, void *user,
                      uint64_t *prev_blockno)
{
	uint64_t height_required = tree_height((off + size + BPFS_BLOCK_SIZE - 1) / BPFS_BLOCK_SIZE);
	uint64_t new_blockno = *prev_blockno;
	unsigned root_off = ((uintptr_t) root) % BPFS_BLOCK_SIZE;
	uint64_t max_nblocks;
	uint64_t child_new_blockno;
	enum commit child_commit;
	int r;

	/* convenience to help callers avoid get_inode() calls */
	if (off == BPFS_EOF)
		off = root->nbytes;
	if (size == BPFS_EOF)
		size = root->nbytes - off;

	if (root->height < height_required)
	{
		assert(commit != COMMIT_NONE);
		r = tree_change_height(root, height_required, COMMIT_ATOMIC, &new_blockno);
		if (r < 0)
			return r;
		root = (struct bpfs_tree_root*) (get_block(new_blockno) + root_off);
	}
	child_new_blockno = root->addr;
	max_nblocks = tree_max_nblocks(root->height);

	if (commit == COMMIT_NONE)
		child_commit = COMMIT_NONE;
	else if (commit == COMMIT_FREE)
		child_commit = COMMIT_FREE;
	else
	{
		if (off < root->nbytes && root->nbytes < off + size)
			child_commit = COMMIT_COPY; // data needs atomic commit with nbytes
		else
			child_commit = commit;
	}

	if (!root->height)
	{
		assert(off + size <= BPFS_BLOCK_SIZE);
		if (size)
			r = crawl_leaf(root->addr, 0, off, size, root->nbytes, off,
			               child_commit, callback, user, NULL,
			               &child_new_blockno);
		else
			r = 0;
	}
	else
	{
		r = crawl_indir(root->addr, off / BPFS_BLOCK_SIZE,
		                off, size, root->nbytes,
                        off, child_commit, root->height, max_nblocks,
		                callback, user, NULL, &child_new_blockno);
	}

	if (r >= 0)
	{
		bool change_addr = root->addr != child_new_blockno;
		bool change_size = off + size > root->nbytes;
		if (change_addr || change_size)
		{
			bool inplace;
			assert(commit != COMMIT_NONE);
			if (*prev_blockno != new_blockno)
				inplace = true;
			else if (change_addr && change_size)
				inplace = commit == COMMIT_FREE;
			else
			{
				inplace = commit == COMMIT_FREE;
#if COW_OPT
				inplace ||= commit == COMMIT_ATOMIC;
#endif
			}
			if (!inplace)
			{
				unsigned root_off = ((uintptr_t) root) % BPFS_BLOCK_SIZE;
				new_blockno = cow_block_entire(new_blockno);
				if (new_blockno == BPFS_BLOCKNO_INVALID)
					return -ENOSPC;
				root = (struct bpfs_tree_root*) (get_block(new_blockno) + root_off);
			}
			if (change_addr)
				root->addr = child_new_blockno;
			if (change_size)
				root->nbytes = off + size;
		}
	}

	*prev_blockno = new_blockno;
	return r;
}

static int crawl_inodes(uint64_t off, uint64_t size, enum commit commit,
                        crawl_callback callback, void *user)
{
	struct bpfs_tree_root *root = get_inode_root();
	uint64_t child_blockno = bpfs_super->inode_root_addr;
	int r;

	r = crawl_tree(root, off, size, commit, callback, user,
	               &child_blockno);

	if (r >= 0 && child_blockno != bpfs_super->inode_root_addr)
	{
		assert(commit != COMMIT_NONE);
		bpfs_super->inode_root_addr = child_blockno;
	}

	return r;
}

struct callback_crawl_inode_data {
	crawl_callback_inode callback;
	void *user;
};

static int callback_crawl_inode(uint64_t blockoff, char *block,
                                unsigned off, unsigned size, unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *ccid_void, uint64_t *blockno)
{
	struct callback_crawl_inode_data *ccid = (struct callback_crawl_inode_data*) ccid_void;
	struct bpfs_inode *inode = (struct bpfs_inode*) (block + off);

	assert(size == sizeof(struct bpfs_inode));

	return ccid->callback(block, off, inode, commit, ccid->user, blockno);
}

static int crawl_inode(uint64_t ino, enum commit commit,
                       crawl_callback_inode callback, void *user)
{
	struct callback_crawl_inode_data ccid = {callback, user};
	uint64_t ino_off;

	xcall(get_inode_offset(ino, &ino_off));

	return crawl_inodes(ino_off, sizeof(struct bpfs_inode), commit,
	                    callback_crawl_inode, &ccid);
}

struct callback_crawl_data_data {
	uint64_t off;
	uint64_t size;
	crawl_callback callback;
	void *user;
};

static int callback_crawl_data(char *block, unsigned off,
                               struct bpfs_inode *inode, enum commit commit,
                               void *ccdd_void, uint64_t *blockno)
{
	struct callback_crawl_data_data *ccdd = (struct callback_crawl_data_data*) ccdd_void;

	return crawl_tree(&inode->root, ccdd->off, ccdd->size, commit,
	                  ccdd->callback, ccdd->user, blockno);
}

static int crawl_data(uint64_t ino, uint64_t off, uint64_t size,
                       enum commit commit,
                       crawl_callback callback, void *user)
{
	struct callback_crawl_data_data ccdd = {off, size, callback, user};

	return crawl_inode(ino, commit, callback_crawl_data, &ccdd);
}

#if 0
struct callback_crawl_inode_2_data {
	struct inode_data {
		uint64_t ino;
		uint64_t ino_off;
		uint64_t off;
		uint64_t size;
	} inodes[2];
	crawl_callback callback;
	void *user;
};

static void inode_data_fill(struct inode_data *id,
                            uint64_t ino, uint64_t off, uint64_t size)
{
	id->ino = ino;
	xcall(get_inode_offset(ino, &id->ino_off));
	id->off = off;
	id->size = size;
}

static int callback_crawl_inode_2(uint64_t blockoff, char *block,
                                  unsigned off, unsigned size, unsigned valid,
                                  uint64_t crawl_start, enum commit commit,
                                  void *cci2d_void, uint64_t *blockno)
{
	struct callback_crawl_inode_2_data *cci2d = (struct callback_crawl_inode_2_data*) cci2d_void;
	uint64_t first_offset = blockoff * BPFS_BLOCK_SIZE + off;
	uint64_t last_offset = first_offset + size - sizeof(struct bpfs_inode);
	struct inode_data *id;
	uint64_t inode_off;
	struct bpfs_inode *inode;

	if (first_offset == cci2d->inodes[0].ino_off)
	{
		id = &cci2d->inodes[0];
		inode_off = off;
	}
	else if (last_offset == cci2d->inodes[1].ino_off)
	{
		id = &cci2d->inodes[1];
		inode_off = off + size - sizeof(struct bpfs_inode);
	}
	else
		return 0;

	inode = (struct bpfs_inode*) (block + inode_off);

	return crawl_tree(&inode->root, id->off, id->size, commit,
	                  cci2d->callback, cci2d->user, blockno);
}


static int crawl_inode_2(uint64_t ino_1, uint64_t off_1, uint64_t size_1,
                         uint64_t ino_2, uint64_t off_2, uint64_t size_2,
                         enum commit commit,
                         crawl_callback callback, void *user)
{
	struct callback_crawl_inode_2_data cci2d;
	uint64_t ino_start, ino_end, ino_size;
	uint64_t new_blockno = bpfs_super->inode_root_addr;
	struct bpfs_tree_root *root = get_inode_root();
	int r;

	if (ino_1 <= ino_2)
	{
		inode_data_fill(&cci2d.inodes[0], ino_1, off_1, size_1);
		inode_data_fill(&cci2d.inodes[1], ino_2, off_2, size_2);
	}
	else
	{
		inode_data_fill(&cci2d.inodes[1], ino_1, off_1, size_1);
		inode_data_fill(&cci2d.inodes[0], ino_2, off_2, size_2);
	}
	cci2d.callback = callback;
	cci2d.user = user;

	ino_start = cci2d.inodes[0].ino_off;
	ino_end = cci2d.inodes[1].ino_off + sizeof(struct bpfs_inode);
	ino_size = ino_end - ino_start;

	r = crawl_tree(root, ino_start, ino_size, commit,
	               callback_crawl_inode, &cci2d, &new_blockno);
	if (r >= 0 && bpfs_super->inode_root_addr != new_blockno)
	{
		xassert(commit != COMMIT_NONE);
		bpfs_super->inode_root_addr = new_blockno;
	}
	return r;
}
#endif


//
// commit, abort, and recover

#if !SCSP_ENABLED
static int recover_superblock(void)
{
	struct bpfs_super *super_2 = (struct bpfs_super*) (((char*) bpfs_super) + BPFS_BLOCK_SIZE);

	if (bpfs_super->commit_mode == BPFS_COMMIT_SCSP)
		return 0;

	if (bpfs_super->commit_mode != BPFS_COMMIT_SP)
		return -1;

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

static struct bpfs_super *persistent_super;
static struct bpfs_super staged_super;

static void persist_superblock(void)
{
	struct bpfs_super *persistent_super_2 = (struct bpfs_super*) (((char*) persistent_super) + BPFS_BLOCK_SIZE);

	assert(bpfs_super == &staged_super);

#if DETECT_NONCOW_WRITES
	{
		size_t len = BPFS_BLOCK_SIZE * 2; /* two super blocks */
		xsyscall(mprotect(bpram, len, PROT_READ | PROT_WRITE));
	}
#endif

	staged_super.inode_root_addr_2        = staged_super.inode_root_addr;
	persistent_super->inode_root_addr     = staged_super.inode_root_addr;
	persistent_super->inode_root_addr_2   = staged_super.inode_root_addr;
	persistent_super_2->inode_root_addr   = staged_super.inode_root_addr;
	persistent_super_2->inode_root_addr_2 = staged_super.inode_root_addr;

	assert(!memcmp(persistent_super, &staged_super, sizeof(staged_super)));
	assert(!memcmp(persistent_super_2, &staged_super, sizeof(staged_super)));

#if DETECT_NONCOW_WRITES
	{
		size_t len = BPFS_BLOCK_SIZE * 2; /* two super blocks */
		xsyscall(mprotect(bpram, len, PROT_READ));
	}
#endif
}
#endif

static void bpfs_abort(void)
{
	abort_blocks();
	abort_inodes();
}

static void bpfs_commit(void)
{
#if !SCSP_ENABLED
	persist_superblock();
#endif

	commit_blocks();
	commit_inodes();
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
		set_block(indir->addr[no]);
		if (height > 1)
		{
			struct bpfs_indir_block *child_indir = (struct bpfs_indir_block*) get_block(indir->addr[no]);
			uint64_t child_valid;
			if (no < lastno)
				child_valid = child_max_nbytes;
			else
				child_valid = valid % child_max_nbytes;
			discover_indir_allocations(child_indir, height - 1,
			                           child_max_nblocks, child_valid);
		}
	}
}

static void discover_tree_allocations(struct bpfs_tree_root *root)
{
	// TODO: better to call crawl_tree()?
	if (root->nbytes)
	{
		set_block(root->addr);
		if (root->height)
		{
			struct bpfs_indir_block *indir = (struct bpfs_indir_block*) get_block(root->addr);
			uint64_t max_nblocks = tree_max_nblocks(root->height);
			discover_indir_allocations(indir, root->height, max_nblocks,
			                           root->nbytes);
		}
	}
}

static void discover_inode_allocations(uint64_t ino);

static int callback_discover_inodes(uint64_t blockoff, char *block,
                                    unsigned off, unsigned size,
                                    unsigned valid, uint64_t crawl_start,
                                    enum commit commit, void *user,
                                    uint64_t *blockno)
{
	unsigned start = off;
	while (off + BPFS_DIRENT_MIN_LEN <= start + size)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		if (!strcmp(dirent->name, ".."))
			continue;
		discover_inode_allocations(dirent->ino);
	}
	return 0;
}

static void discover_inode_allocations(uint64_t ino)
{
	struct bpfs_inode *inode = get_inode(ino);

	set_inode(ino);

	// TODO: combine the inode and block discovery loops?
	discover_tree_allocations(&inode->root);
	if (BPFS_S_ISDIR(inode->mode))
		xcall(crawl_data(ino, 0, BPFS_EOF, COMMIT_NONE,
		                 callback_discover_inodes, NULL));
}

static int init_allocations(void)
{
	uint64_t i;
	xcall(init_block_allocations());
	xcall(init_inode_allocations());
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	for (i = 1; i < BPFS_BLOCKNO_FIRST_ALLOC; i++)
		set_block(i);
	set_block(bpfs_super->inode_root_addr);
	discover_tree_allocations(get_inode_root());
	discover_inode_allocations(BPFS_INO_ROOT);
	return 0;
}

static void destroy_allocations(void)
{
	destroy_inode_allocations();
	destroy_block_allocations();
}


//
// misc internal functions

static uint64_t bpram_blockno(const void *x)
{
	const char *c = (const char*) x;
	if (c < bpram || bpram + bpram_size <= c)
		return BPFS_BLOCKNO_INVALID;
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return (((uintptr_t) (c - bpram)) / BPFS_BLOCK_SIZE) + 1;
}

static int bpfs_stat(fuse_ino_t ino, struct stat *stbuf)
{
	struct bpfs_inode *inode = get_inode(ino);
	if (!inode)
		return -ENOENT;
	memset(stbuf, 0, sizeof(stbuf));
	stbuf->st_ino = ino;
	stbuf->st_nlink = inode->nlinks;
	stbuf->st_mode = b2f_mode(inode->mode);
	stbuf->st_uid = inode->uid;
	stbuf->st_gid = inode->gid;
	stbuf->st_size = inode->root.nbytes;
	stbuf->st_blocks = NBLOCKS_FOR_NBYTES(inode->root.nbytes)
	                   * BPFS_BLOCK_SIZE / 512;
	stbuf->st_atime = inode->atime.sec;
	stbuf->st_mtime = inode->mtime.sec;
	stbuf->st_ctime = inode->ctime.sec;
	return 0;
}


static int callback_find_dirent(uint64_t blockoff, char *block,
                                unsigned off, unsigned size, unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *sd_void, uint64_t *blockno)
{
	struct str_dirent *sd = (struct str_dirent*) sd_void;
	unsigned start = off;
	while (off + BPFS_DIRENT_MIN_LEN <= start + size)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		if (sd->str.len == dirent->name_len
		    && !memcmp(sd->str.str, dirent->name, sd->str.len))
		{
			sd->dirent_off = blockoff * BPFS_BLOCK_SIZE
			                         + off - dirent->rec_len;
			sd->dirent = dirent;
			return 1;
		}
	}
	return 0;
}

static int find_dirent(uint64_t parent_ino, struct str_dirent *sd)
{
	int r;
	assert(BPFS_S_ISDIR(get_inode(parent_ino)->mode));

	r = crawl_data(parent_ino, 0, BPFS_EOF, COMMIT_NONE,
	               callback_find_dirent, sd);
	if (r < 0)
		return r;
	else if (!r)
		return -ENOENT;
	assert(r == 1);
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
	assert(dirent_off + BPFS_DIRENT_MIN_LEN <= get_inode(parent_ino)->root.nbytes);

	r = crawl_data(parent_ino, dirent_off, 1, COMMIT_NONE,
	               callback_get_dirent, &dirent);
	if (r < 0)
		return NULL;
	return dirent;
}

static int callback_dirent_plug(uint64_t blockoff, char *block,
                                unsigned off, unsigned size,  unsigned valid,
                                uint64_t crawl_start, enum commit commit,
                                void *sd_void, uint64_t *blockno)
{
	struct str_dirent *sd = (struct str_dirent*) sd_void;
	struct bpfs_dirent *dirent;
	unsigned start = off;
	uint64_t min_hole_size = BPFS_DIRENT_LEN(sd->str.len);

	while (off + min_hole_size <= start + size)
	{
		assert(!(off % BPFS_DIRENT_ALIGN));
		dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			goto found;
		}
		if (dirent->ino == BPFS_INO_INVALID && dirent->rec_len >= min_hole_size)
		{
			// empty dirent
			goto found;
		}
		off += dirent->rec_len;
	}
	return 0;

  found:
	assert(commit != COMMIT_NONE);

	if (commit != COMMIT_FREE)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
		dirent = (struct bpfs_dirent*) (block + off);
		*blockno = new_blockno;
	}

	// TODO: set file_type here
	if (!dirent->rec_len)
		dirent->rec_len = min_hole_size;
	dirent->name_len = sd->str.len;
	memcpy(dirent->name, sd->str.str, sd->str.len);
	sd->dirent_off = blockoff * BPFS_BLOCK_SIZE + off;
	sd->dirent = dirent;
	return 1;
}

static int callback_dirent_append(uint64_t blockoff, char *block,
                                  unsigned off, unsigned size,  unsigned valid,
                                  uint64_t crawl_start, enum commit commit,
                                  void *sd_void, uint64_t *blockno)
{
	struct str_dirent *sd = (struct str_dirent*) sd_void;

	assert(!off && size == BPFS_BLOCK_SIZE);
	assert(crawl_start == blockoff * BPFS_BLOCK_SIZE);
	assert(!valid);
	assert(commit != COMMIT_NONE);
	assert(commit == COMMIT_FREE);

	static_assert(BPFS_INO_INVALID == 0);
	memset(block, 0, BPFS_BLOCK_SIZE);

	sd->dirent_off = blockoff * BPFS_BLOCK_SIZE;
	sd->dirent = (struct bpfs_dirent*) block;

	// TODO: set file_type here
	if (!sd->dirent->rec_len)
		sd->dirent->rec_len = BPFS_DIRENT_LEN(sd->str.len);
	sd->dirent->name_len = sd->str.len;
	memcpy(sd->dirent->name, sd->str.str, sd->str.len);

	return 0;
}

static int alloc_dirent(uint64_t parent_ino, struct str_dirent *sd)
{
	int r;

	r = crawl_data(parent_ino, 0, BPFS_EOF, COMMIT_ATOMIC,
	               callback_dirent_plug, sd);
	if (r < 0)
		return r;

	if (!r)
	{
		r = crawl_data(parent_ino, BPFS_EOF, BPFS_BLOCK_SIZE,
		               COMMIT_ATOMIC, callback_dirent_append, sd);
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
	uint64_t *ino = (uint64_t*) ino_void;
	struct bpfs_dirent *dirent;

	assert(commit != COMMIT_NONE);

	if (commit != COMMIT_FREE)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
		*blockno = new_blockno;
	}
	dirent = (struct bpfs_dirent*) (block + off);

	dirent->ino = *ino;

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

	if (commit != COMMIT_FREE)
	{
		uint64_t new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
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

	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	if (cadd->dir)
	{
		if (cadd->add)
			inode->nlinks++;
		else
			inode->nlinks--;
	}

	dirent_callback = cadd->add ? callback_set_dirent_ino : callback_clear_dirent_ino;
	r = crawl_tree(&inode->root, cadd->dirent_off, 1,
	               COMMIT_COPY, dirent_callback, &cadd->ino, &new_blockno);
	if (r < 0)
		return r;

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

	// TODO: only for SP?
	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->generation++;
	assert(inode->generation); // not allowed to repeat within a fuse session
	inode->mode = f2b_mode(ciid->mode);
	inode->uid = ciid->ctx->uid;
	inode->gid = ciid->ctx->gid;
	inode->nlinks = 1;
	inode->mtime = inode->ctime = inode->atime = BPFS_TIME_NOW();
	inode->root.height = 0;
	inode->root.nbytes = 0;
	// TODO: flags
	inode->root.addr = BPFS_BLOCKNO_INVALID;

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

	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->ctime = inode->mtime = *new_time;

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
	int r;

	assert(!!link == !!S_ISLNK(mode));

	if (name_len > BPFS_DIRENT_MAX_NAME_LEN)
		return -ENAMETOOLONG;

	if (!get_inode(parent_ino))
		return -ENOENT;
	assert(BPFS_S_ISDIR(get_inode(parent_ino)->mode));

	if (!find_dirent(parent_ino, &sd))
		return -EEXIST;

	ino = alloc_inode();
	if (ino == BPFS_INO_INVALID)
		return -ENOSPC;

	if ((r = alloc_dirent(parent_ino, &sd)) < 0)
		return r;

	r = crawl_inode(ino, COMMIT_COPY, callback_init_inode, &ciid);
	if (r < 0)
		return r;

	r = crawl_inode(parent_ino, COMMIT_COPY,
	                callback_set_cmtime, &get_inode(ino)->mtime);
	if (r < 0)
		return r;

	if (S_ISDIR(mode) || S_ISLNK(mode))
	{
		// inode's block freshly allocated for SP and inode ignored for SCSP
		struct bpfs_inode *inode = get_inode(ino);
		assert(inode);

		inode->root.addr = alloc_block();
		if (inode->root.addr == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;

		if (S_ISDIR(mode))
		{
			struct bpfs_dirent *ndirent;

			inode->root.nbytes = BPFS_BLOCK_SIZE;

			ndirent = (struct bpfs_dirent*) get_block(inode->root.addr);
			assert(ndirent);

			inode->nlinks++;

			static_assert(BPFS_INO_INVALID == 0);
			memset(ndirent, 0, BPFS_BLOCK_SIZE);
			ndirent->ino = parent_ino;
			ndirent->file_type = BPFS_TYPE_DIR;
			strcpy(ndirent->name, "..");
			ndirent->name_len = strlen(ndirent->name) + 1;
			ndirent->rec_len = BPFS_DIRENT_LEN(ndirent->name_len);
		}
		else if (S_ISLNK(mode))
		{
			inode->root.nbytes = strlen(link) + 1;
			assert(inode->root.nbytes <= BPFS_BLOCK_SIZE); // else use crawler
			memcpy(get_block(inode->root.addr), link, inode->root.nbytes);
		}
	}

	// dirent's block is freshly allocated or already copied
	sd.dirent->file_type = f2b_filetype(mode);

	// Set sd.dirent->ino and, if S_ISDIR, increment parent->nlinks.
	// Could do this atomically if we didn't store parent->nlinks.
	// TODO: optimize away this COW?
	cadd.dirent_off = sd.dirent_off;
	cadd.ino = ino;
	r = crawl_inode(parent_ino, COMMIT_COPY, callback_addrem_dirent, &cadd);
	if (r < 0)
		return r;

	sd.dirent = get_dirent(parent_ino, sd.dirent_off);
	assert(sd.dirent);

	*pdirent = sd.dirent;
	return 0;
}


//
// fuse interface

static void fuse_init(void *userdata, struct fuse_conn_info *conn)
{
	static_assert(FUSE_ROOT_ID == BPFS_INO_ROOT);
	Dprintf("%s()\n", __FUNCTION__);
	bpfs_commit();
}

static void fuse_destroy(void *userdata)
{
	Dprintf("%s()\n", __FUNCTION__);
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
	stv.f_bsize = BPFS_BLOCK_SIZE;
	stv.f_frsize = BPFS_BLOCK_SIZE;
	static_assert(sizeof(stv.f_blocks) >= sizeof(bpfs_super->nblocks));
	stv.f_blocks = bpfs_super->nblocks;
	stv.f_bfree = block_bitmap.nfree;
	stv.f_bavail = stv.f_bfree; // NOTE: no space reserved for root
	stv.f_files = inode_bitmap.ntotal - inode_bitmap.nfree;
	stv.f_ffree = inode_bitmap.nfree;
	stv.f_favail = stv.f_ffree; // NOTE: no space reserved for root
	memset(&stv.f_fsid, 0, sizeof(stv.f_fsid)); // TODO: good enough?
	stv.f_flag = 0; // TODO: check for flags (see mount(8))
	stv.f_namemax = BPFS_DIRENT_MAX_NAME_LEN;

	bpfs_commit();
	xcall(fuse_reply_statfs(req, &stv));
}

static void fill_fuse_entry(const struct bpfs_dirent *dirent, struct fuse_entry_param *e)
{
	memset(e, 0, sizeof(e));
	e->ino = dirent->ino;
	e->generation = get_inode(dirent->ino)->generation;
	e->attr_timeout = STDTIMEOUT;
	e->entry_timeout = STDTIMEOUT;
	xcall(bpfs_stat(e->ino, &e->attr));
}

static void fuse_lookup(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	struct str_dirent sd = {{name, strlen(name) + 1}, BPFS_EOF, NULL};
	struct fuse_entry_param e;
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, &sd);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(sd.dirent, &e);
	bpfs_commit();
	xcall(fuse_reply_entry(req, &e));
}


#if 0
static void fuse_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
	Dprintf("%s(ino = %lu, nlookup = %lu)\n", __FUNCTION__, ino, nlookup);
	bpfs_commit();
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
	uint64_t new_blockno = *blockno;

	// NOTE: don't need to do all of these atomically?
	// but do want to preserve syscall atomicity?

	assert(commit != COMMIT_NONE);

	// TODO: avoid COW when COMMIT_ATOMIC and can change atomically
	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(*blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	if (to_set & FUSE_SET_ATTR_MODE)
		inode->mode = f2b_mode(attr->st_mode);
	if (to_set & FUSE_SET_ATTR_UID)
		inode->uid = attr->st_uid;
	if (to_set & FUSE_SET_ATTR_GID)
		inode->gid = attr->st_gid;
	if (to_set & FUSE_SET_ATTR_SIZE)
	{
		uint64_t new_blockno2 = new_blockno;
		int r;

		if (attr->st_size < inode->root.nbytes)
			truncate_block_free(&inode->root, attr->st_size);
		// TODO: expand file if attr->st_size > inode->root.nbytes
		// probably do this on the read side (detect empty blocks)

		inode->root.nbytes = attr->st_size;

		// TODO: change FREE to ATOMIC as part of optimizing to COW
		// TODO: free blocks not along the trunk (or already happening?)
		r = tree_change_height(&inode->root, tree_height(attr->st_size),
		                       COMMIT_FREE, &new_blockno2);
		if (r < 0)
			return r;
		assert(new_blockno == new_blockno2);
	}
	if (to_set & FUSE_SET_ATTR_ATIME)
		inode->atime.sec = attr->st_atime;
	if (to_set & FUSE_SET_ATTR_MTIME)
		inode->mtime.sec = attr->st_mtime;
	inode->ctime = BPFS_TIME_NOW();

	if (to_set & FUSE_SET_ATTR_SIZE)
		inode->mtime = inode->ctime;

	*blockno = new_blockno;
	return 0;
}

static void fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                         int to_set, struct fuse_file_info *fi)
{
	struct callback_setattr_data csd = {attr, to_set};
	struct stat stbuf;
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	// Why are bits 6, 7, and 8 set?
	// in fuse 2.8.1, 7 is atime_now and 8 is mtime_now. 6 is skipped.
	// assert(!(to_set & ~(FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID | FUSE_SET_ATTR_SIZE | FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)));

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
	static_assert(BPFS_DIRENT_MAX_NAME_LEN <= BPFS_BLOCK_SIZE);
	assert(inode->root.nbytes <= BPFS_BLOCK_SIZE);

	bpfs_commit();
	xcall(fuse_reply_readlink(req, get_block(inode->root.addr)));
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

static int do_unlink(uint64_t parent_ino, uint64_t dirent_off, uint64_t child_ino)
{
	struct bpfs_time time_now = BPFS_TIME_NOW();
	struct callback_addrem_dirent_data cadd;
	struct bpfs_inode *child;
	int r;

	child = get_inode(child_ino);
	assert(child);
	cadd = (struct callback_addrem_dirent_data) {false, dirent_off, BPFS_INO_INVALID, BPFS_S_ISDIR(child->mode)};
	r = crawl_inode(parent_ino, COMMIT_ATOMIC,
	                callback_addrem_dirent, &cadd);
	if (r < 0)
		return r;

	r = crawl_inode(parent_ino, COMMIT_ATOMIC,
	                callback_set_cmtime, &time_now);
	if (r < 0)
		return r;

	child = get_inode(child_ino);
	assert(child);
	truncate_block_free(&child->root, 0);
	free_inode(child_ino);

	return 0;
}

static void fuse_unlink(fuse_req_t req, fuse_ino_t parent_ino,
                        const char *name)
{
	struct str_dirent sd = {{name, strlen(name) + 1}, BPFS_EOF, NULL};
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, &sd);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	r = do_unlink(parent_ino, sd.dirent_off, sd.dirent->ino);
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

static int callback_empty_dir(uint64_t blockoff, char *block,
                              unsigned off, unsigned size, unsigned valid,
                              uint64_t crawl_start, enum commit commit,
                              void *ppi, uint64_t *blockno)
{
	uint64_t parent_ino = *(fuse_ino_t*) ppi;
	unsigned start = off;
	while (off + BPFS_DIRENT_MIN_LEN <= start + size)
	{
		struct bpfs_dirent *dirent = (struct bpfs_dirent*) (block + off);
		if (!dirent->rec_len)
		{
			// end of directory entries in this block
			break;
		}
		off += dirent->rec_len;
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		if (dirent->ino == parent_ino)
			continue;
		return 1;
	}
	return 0;
}

static void fuse_rmdir(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	struct str_dirent sd = {{name, strlen(name) + 1}, BPFS_EOF, NULL};
	int r;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	r = find_dirent(parent_ino, &sd);
	if (r < 0)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, -r));
		return;
	}

	r = crawl_data(sd.dirent->ino, 0, BPFS_EOF, COMMIT_NONE,
	               callback_empty_dir, &parent_ino);
	xassert(r >= 0);
	if (r == 1)
	{
		bpfs_abort();
		xcall(fuse_reply_err(req, ENOTEMPTY));
		return;
	}

	r = do_unlink(parent_ino, sd.dirent_off, sd.dirent->ino);
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

static int callback_set_ctime(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->ctime = *new_time;

	*blockno = new_blockno;
	return 0;
}

static void fuse_rename(fuse_req_t req,
                        fuse_ino_t src_parent_ino, const char *src_name,
                        fuse_ino_t dst_parent_ino, const char *dst_name)
{
	struct str_dirent src_sd = {{src_name, strlen(src_name) + 1}, BPFS_EOF, NULL};
	struct str_dirent dst_sd = {{dst_name, strlen(dst_name) + 1}, BPFS_EOF, NULL};
	struct bpfs_time time_now = BPFS_TIME_NOW();
	uint64_t invalid_ino = BPFS_INO_INVALID;
	uint64_t unlinked_ino = BPFS_INO_INVALID;
	int r;

	Dprintf("%s(src_parent_ino = %lu, src_name = '%s',"
	        " dst_parent_ino = %lu, dst_name = '%s')\n",
	        __FUNCTION__, src_parent_ino, src_name, dst_parent_ino, dst_name);

	r = find_dirent(src_parent_ino, &src_sd);
	if (r < 0)
		goto abort;

	(void) find_dirent(dst_parent_ino, &dst_sd);

	if (dst_sd.dirent)
	{
		// TODO: check that types match?
		unlinked_ino = dst_sd.dirent->ino;
	}
	else
	{
		r = alloc_dirent(dst_parent_ino, &dst_sd);
		if (r < 0)
			goto abort;
		// May be necessary:
		src_sd.dirent = get_dirent(src_parent_ino, src_sd.dirent_off);
		assert(src_sd.dirent);

		assert(block_freshly_alloced(bpram_blockno(dst_sd.dirent)));
		dst_sd.dirent->file_type = src_sd.dirent->file_type;
	}

#if 0
	// TODO: combine alloc_dirent() into this crawl
	crawl_inode_2(src_parent_ino, src_dirent_off, src_dirent->rec_len,
				  dst_parent_ino, dst_dirent_off, dst_dirent->rec_len,
				  COMMIT_ATOMIC, callback_rename);
#endif

	r = crawl_data(dst_parent_ino, dst_sd.dirent_off, 1, COMMIT_COPY,
	               callback_set_dirent_ino, &src_sd.dirent->ino);
	if (r < 0)
		goto abort;
	r = crawl_data(src_parent_ino, src_sd.dirent_off, 1, COMMIT_COPY,
	               callback_set_dirent_ino, &invalid_ino);
	if (r < 0)
		goto abort;

	r = crawl_inode(dst_parent_ino, COMMIT_COPY, callback_set_cmtime, &time_now);
	if (r < 0)
		goto abort;
	r = crawl_inode(src_parent_ino, COMMIT_COPY, callback_set_cmtime, &time_now);
	if (r < 0)
		goto abort;
	if (dst_sd.dirent->file_type == BPFS_TYPE_DIR)
	{
		// FIXME: need to change the ino for inode's ".."?
		r = crawl_inode(dst_sd.dirent->ino, COMMIT_COPY,
		                callback_set_ctime, &time_now);
		if (r < 0)
			goto abort;
	}

	if (unlinked_ino != BPFS_INO_INVALID)
	{
		// like do_unlink(), but parent [cm]time and dirent already updated:
		truncate_block_free(&get_inode(unlinked_ino)->root, 0);
		free_inode(unlinked_ino);
	}

	bpfs_commit();
	xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
	return;

  abort:
	bpfs_abort();
	xcall(fuse_reply_err(req, -r));
}

#if 0
static void fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent_ino,
                      const char *name)
{
	Dprintf("%s(ino = %lu, parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, ino, parent_ino, name);
	bpfs_commit();
}


static void fuse_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
	bpfs_commit();
}
#endif

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
	const unsigned start = off;
	while (off + BPFS_DIRENT_MIN_LEN <= start + size)
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
		                  1 + blockoff * BPFS_BLOCK_SIZE + off);
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

	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->atime = *new_time;

	*blockno = new_blockno;
	return 0;
}

// FIXME: is readdir() supposed to not notice changes made after the opendir?
static void fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t max_size,
                         off_t off, struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	struct readdir_params params = {req, max_size, 0, NULL};
	struct bpfs_time time_now = BPFS_TIME_NOW();
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ")\n",
	        __FUNCTION__, ino, off);

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

	if (off == 0)
	{
		struct stat stbuf;
		size_t fuse_dirent_size;
		char name[] = ".";

		memset(&stbuf, 0, sizeof(stbuf));
		stbuf.st_ino = ino;
		stbuf.st_mode = S_IFDIR;
		off++;

		fuse_dirent_size = fuse_add_direntry(req, NULL, 0,
		                                     name, NULL, 0);
		xassert(fuse_dirent_size <= params.max_size); // should be true...
		params.total_size += fuse_dirent_size;
		params.buf = (char*) realloc(params.buf, params.total_size);
		if (!params.buf)
		{
			r = -ENOMEM;
			goto abort;
		}

		fuse_add_direntry(req, params.buf, params.total_size, name, &stbuf,
		                  off);
	}

	r = crawl_data(ino, off - 1, BPFS_EOF, COMMIT_NONE,
	               callback_readdir, &params);
	if (r < 0)
		goto abort;

	r = crawl_inode(ino, COMMIT_COPY, callback_set_atime, &time_now);
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
	printf("fsync(ino = %" PRIu64 ", datasync = %d): not yet implemented\n",
	       ino, datasync);
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
	assert(BPFS_S_ISREG(inode->mode));

	size = MIN(size, inode->root.nbytes - off);
	first_blockoff = off / BPFS_BLOCK_SIZE;
	last_blockoff = (off + size - 1) / BPFS_BLOCK_SIZE;
	nblocks = last_blockoff - first_blockoff + 1;
	iov = calloc(nblocks, sizeof(*iov));
	if (!iov)
	{
		r = -ENOMEM;
		goto abort;
	}
	r = crawl_data(ino, off, size, COMMIT_NONE, callback_read, iov);
	assert(r >= 0);

	r = crawl_inode(ino, COMMIT_COPY, callback_set_atime, &time_now);
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
	if (commit == COMMIT_COPY || (commit == COMMIT_ATOMIC && size > ATOMIC_SIZE))
	{
		uint64_t newno = cow_block(*new_blockno, off, size, valid);
		if (newno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		*new_blockno = newno;
		block = get_block(newno);
	}

	// TODO: if size <= ATOMIC_SIZE, does memcpy() make exactly one write?
	memcpy(block + off, buf + buf_offset, size);
	return 0;
}


static int callback_set_mtime(char *block, unsigned off,
                              struct bpfs_inode *inode, enum commit commit,
                              void *new_time_void, uint64_t *blockno)
{
	struct bpfs_time *new_time = (struct bpfs_time*) new_time_void;
	uint64_t new_blockno = *blockno;

	assert(commit != COMMIT_NONE);

	if (commit != COMMIT_FREE)
	{
		new_blockno = cow_block_entire(new_blockno);
		if (new_blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		block = get_block(new_blockno);
	}
	inode = (struct bpfs_inode*) (block + off);

	inode->mtime = *new_time;

	*blockno = new_blockno;
	return 0;
}

static void fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
{
	int r;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ", size = %zu)\n",
	        __FUNCTION__, ino, off, size);

	// crawl won't modify buf; cast away const only because of crawl's type
	r = crawl_data(ino, off, size, COMMIT_ATOMIC,
	               callback_write, (char*) buf);

	if (r >= 0)
	{
		struct bpfs_time time_now = BPFS_TIME_NOW();
		// TODO: for SCSP: what to do if this fails?
		r = crawl_inode(ino, COMMIT_ATOMIC, callback_set_mtime, &time_now);
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
//	ADD_FUSE_CALLBACK(link); // does unlink(file with #links > 1) require CoW?

//	ADD_FUSE_CALLBACK(setxattr);
//	ADD_FUSE_CALLBACK(getxattr);
//	ADD_FUSE_CALLBACK(listxattr);
//	ADD_FUSE_CALLBACK(removexattr);

//	ADD_FUSE_CALLBACK(opendir);
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
	xassert(!(((uintptr_t) bpram) % BPFS_BLOCK_SIZE));
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
	int r;
	assert(!bpram && !bpram_size);
	// some code assumes block memory address are block aligned
	r = posix_memalign((void**) &bpram, BPFS_BLOCK_SIZE, size);
	xassert(!r); // note: posix_memalign() returns positives on error
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

int main(int argc, char **argv)
{
	void (*destroy_bpram)(void);
	int r = -1;

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

	bpfs_super = (struct bpfs_super*) bpram;

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

#if SCSP_ENABLED
	bpfs_super->commit_mode = BPFS_COMMIT_SCSP;
	((struct bpfs_super*) (bpram + BPFS_BLOCK_SIZE))->commit_mode = BPFS_COMMIT_SCSP;
#else
	bpfs_super->commit_mode = BPFS_COMMIT_SP;
	((struct bpfs_super*) (bpram + BPFS_BLOCK_SIZE))->commit_mode = BPFS_COMMIT_SP;
	persistent_super = bpfs_super;
	staged_super = *bpfs_super;
	bpfs_super = &staged_super;
#endif

	xcall(init_allocations());

	memmove(argv + 1, argv + 3, (argc - 2) * sizeof(*argv));
	argc -= 2;

	{
		struct fuse_args fargs = FUSE_ARGS_INIT(argc, argv);
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

	destroy_allocations();

	destroy_bpram();

	return r;
}
