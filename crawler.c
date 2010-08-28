/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "crawler.h"
#include "bpfs.h"
#include "indirect_cow.h"
#include "util.h"

#include <sys/mman.h>
#include <unistd.h>


//
// Core crawler

static char zero_block[BPFS_BLOCK_SIZE]
	__attribute__((aligned(BPFS_BLOCK_SIZE)));

static int crawl_leaf(uint64_t prev_blockno, uint64_t blockoff,
                      unsigned off, unsigned size, unsigned valid,
                      uint64_t crawl_start, enum commit commit,
					  crawl_callback callback, void *user,
					  crawl_blockno_callback bcallback,
					  uint64_t *new_blockno)
{
	uint64_t blockno = prev_blockno;
	bool is_hole = blockno == BPFS_BLOCKNO_INVALID && commit == COMMIT_NONE;
	uint64_t child_blockno;
	int r;

	assert(crawl_start / BPFS_BLOCK_SIZE <= blockoff);
	assert(off < BPFS_BLOCK_SIZE);
	assert(off + size <= BPFS_BLOCK_SIZE);
	assert(valid <= BPFS_BLOCK_SIZE);

	if (commit != COMMIT_NONE && blockno == BPFS_BLOCKNO_INVALID)
	{
		blockno = cow_block_hole(off, size, valid);
		if (blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
	}
	child_blockno = blockno;

	if (callback)
	{
		enum commit child_commit = (child_blockno == prev_blockno)
		                           ? commit : COMMIT_FREE;
		char *child_block;
		if (is_hole)
			child_block = zero_block;
		else
			child_block = get_block(child_blockno);

		r = callback(blockoff, child_block, off, size, valid,
		             crawl_start, child_commit, user, &child_blockno);
		if (r >= 0 && prev_blockno != child_blockno)
			*new_blockno = child_blockno;
	}
	else
	{
		if (!is_hole)
		{
			assert(blockno == prev_blockno);
			assert(bcallback);
			bcallback(child_blockno, true);
		}
		r = 0;
	}
	return r;
}

static int crawl_hole(uint64_t blockoff,
                      uint64_t off, uint64_t size, uint64_t valid,
                      uint64_t crawl_start,
                      crawl_callback callback, void *user)
{
	uint64_t off_block = ROUNDDOWN64(off, BPFS_BLOCK_SIZE);
	uint64_t end = off + size;

	assert(crawl_start / BPFS_BLOCK_SIZE <= blockoff);
	assert(off + size <= valid);

	while (off < end)
	{
		unsigned child_off = off % BPFS_BLOCK_SIZE;
		unsigned child_size = MIN(end - off, BPFS_BLOCK_SIZE);
		unsigned child_valid = MIN(valid - off_block, BPFS_BLOCK_SIZE);
		uint64_t child_blockno = BPFS_BLOCKNO_INVALID;
		int r;

		r = callback(blockoff, zero_block, child_off, child_size, child_valid,
		             crawl_start, COMMIT_NONE, user, &child_blockno);
		assert(child_blockno == BPFS_BLOCKNO_INVALID);
		if (r != 0)
			return r;

		blockoff++;
		off_block += BPFS_BLOCK_SIZE;
		off = off_block;
	}

	return 0;
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
	uint64_t firstno = off / child_max_nbytes;
	uint64_t lastno = (off + size - 1) / child_max_nbytes;
	uint64_t validno = (valid + child_max_nbytes - 1) / child_max_nbytes;
	uint64_t in_hole = false;
	bool only_invalid = off >= valid;
	enum commit child_commit;
	uint64_t no;
	int ret = 0;

	switch (commit) {
#if COMMIT_MODE == MODE_BPFS
	case COMMIT_ATOMIC:
		child_commit = (firstno == lastno || only_invalid)
		               ? commit : COMMIT_COPY;
		break;
#endif
	case COMMIT_FREE:
	case COMMIT_COPY:
	case COMMIT_NONE:
		child_commit = commit;
		break;
	}

	if (blockno == BPFS_BLOCKNO_INVALID)
	{
		if (commit == COMMIT_NONE)
			return crawl_hole(blockoff, off, size, valid, crawl_start,
			                  callback, user);

		static_assert(BPFS_BLOCKNO_INVALID == 0);
		blockno = cow_block_hole(firstno * sizeof(indir->addr[0]),
		                         (lastno + 1 - firstno) * sizeof(indir->addr[0]),
		                         validno * sizeof(indir->addr[0]));
		// indirect_cow_block_required(blockno) not required
		if (blockno == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		in_hole = true;
	}
	indir = (struct bpfs_indir_block*) get_block(blockno);

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
			child_blockoff = blockoff + (no - firstno) * child_max_nblocks
			                 - ((off % child_max_nbytes) / BPFS_BLOCK_SIZE);
		}
		assert(blockoff <= child_blockoff);

		if (no == lastno)
			child_size = off + size - (no * child_max_nbytes + child_off);
		else
			child_size = child_max_nbytes - child_off;
		assert(child_size <= size);
		assert(child_size <= child_max_nbytes);

		if (no < validno)
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

		if (!child_valid || in_hole)
			child_blockno = child_new_blockno = BPFS_BLOCKNO_INVALID;
		else
			child_blockno = child_new_blockno = indir->addr[no];

		if (commit != COMMIT_NONE)
			xcall(indirect_cow_parent_push(blockno));
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
		if (commit != COMMIT_NONE)
			indirect_cow_parent_pop(blockno);
		if (r < 0)
			return r;
		if (child_blockno != child_new_blockno || in_hole)
		{
			bool single = firstno == lastno || r == 1;
			assert(commit != COMMIT_NONE);
			if (!(prev_blockno != blockno
			      || (SCSP_OPT_APPEND && only_invalid)
			      || (COMMIT_MODE == MODE_BPFS
			          && ((commit == COMMIT_ATOMIC && single)
			              || !child_valid))))
			{
#if COMMIT_MODE == MODE_BPFS
				// Could avoid the CoW in this case, but it should not occur:
				assert(!(commit == COMMIT_ATOMIC && only_invalid));
#endif
				// TODO: avoid copying data that will be overwritten?
				if ((blockno = cow_block_entire(blockno))
				    == BPFS_BLOCKNO_INVALID)
					return -ENOSPC;
				// indirect_cow_block_required(blockno) not required
				indir = (struct bpfs_indir_block*) get_block(blockno);
			}
			indir->addr[no] = child_new_blockno;
#if INDIRECT_COW
			// Neccessary for plugging a file hole.
			// There may be broader related problems, e.g, when increasing
			// a tree's height?, but so far I've not noticed any breakage.
			// Might alternatively or additionally consider supporting
			// in cow_is_atomically_writable() (!block->orig_blkno -> false)
			// and adding a required() call in/after cow_block_hole().
			if (child_blockno == BPFS_INO_INVALID && block_freshly_alloced(blockno))
				indirect_cow_block_required(blockno);
#endif
			if (SCSP_OPT_APPEND && only_invalid)
				indirect_cow_block_direct(blockno, no * sizeof(*indir->addr),
				                          sizeof(*indir->addr));
		}
		if (r == 1)
		{
			assert(!in_hole); // TODO: set the remaining entries to invalid
			ret = 1;
			break;
		}
	}

	if (bcallback && !off)
	{
		assert(commit == COMMIT_NONE);
		assert(prev_blockno == blockno);
		bcallback(blockno, false);
	}

	if (prev_blockno != blockno)
		*new_blockno = blockno;
	return ret;
}

//
// crawl_blocknos()
// Read-only crawl over the indirect and data blocks in root

void crawl_blocknos(const struct bpfs_tree_root *root,
                    uint64_t off, uint64_t size,
                    crawl_blockno_callback callback)
{
	uint64_t max_nblocks = tree_max_nblocks(tree_root_height(root));
	uint64_t max_nbytes = max_nblocks * BPFS_BLOCK_SIZE;
	uint64_t valid;

	/* convenience */
	if (off == BPFS_EOF)
		off = root->nbytes;
	assert(!off || off < root->nbytes);
	if (size == BPFS_EOF)
		size = root->nbytes - off;
	else
		assert(off + size <= root->nbytes);
	assert(size <= root->nbytes);
	assert(off + size <= root->nbytes);

	if (!(off + size))
		return;

	size = MIN(size, max_nbytes - off);
	valid = MIN(root->nbytes, max_nbytes);


	if (!tree_root_height(root))
	{
		if (!off)
			crawl_leaf(tree_root_addr(root), 0, off, size, valid, off,
			           COMMIT_NONE, NULL, NULL, callback, NULL);
	}
	else
	{
		crawl_indir(tree_root_addr(root), off / BPFS_BLOCK_SIZE,
		            off, size, valid, off, COMMIT_NONE,
		            tree_root_height(root), max_nblocks,
		            NULL, NULL, callback, NULL);
	}
}

//
// crawl_tree()

static int crawl_tree_ref(struct bpfs_tree_root *root, uint64_t off,
                          uint64_t size, enum commit commit,
                          crawl_callback callback, void *user,
                          uint64_t *prev_blockno, bool blockno_refed)
{
	uint64_t new_blockno = *prev_blockno;
	unsigned root_off = block_offset(root);
	uint64_t end;
	uint64_t max_nblocks;
	uint64_t child_new_blockno;
	uint64_t child_size;
	uint64_t child_valid;
	enum commit child_commit;
	bool change_height_holes = false;
	int r;

	/* convenience to help callers avoid get_inode() calls */
	if (off == BPFS_EOF)
		off = root->nbytes;
	if (size == BPFS_EOF)
	{
		assert(root->nbytes >= off);
		size = root->nbytes - off;
	}
	end = off + size;

	assert(commit != COMMIT_NONE || end <= root->nbytes);

	if (commit != COMMIT_NONE)
	{
		uint64_t prev_height = tree_root_height(root);
		uint64_t requested_height = tree_height(NBLOCKS_FOR_NBYTES(end));
		uint64_t new_height = MAXU64(prev_height, requested_height);
#ifndef NDEBUG
		uint64_t new_max_nblocks = tree_max_nblocks(new_height);
#endif
		uint64_t int_valid = MIN(root->nbytes,
		                         BPFS_BLOCK_SIZE
		                         * tree_max_nblocks(new_height));
#ifndef NDEBUG
		uint64_t new_valid = MIN(MAX(root->nbytes, end),
		                         BPFS_BLOCK_SIZE
		                         * tree_max_nblocks(new_height));
#endif

		// FYI:
		assert(end <= new_valid);
		assert(root->nbytes >= new_valid || (root->nbytes < end && end == new_valid));
		assert(root->nbytes <= new_valid || root->nbytes > end);
		assert(root->nbytes != new_valid || root->nbytes >= end);
		assert(new_valid <= BPFS_BLOCK_SIZE * new_max_nblocks);

		if (prev_height < new_height)
		{
			r = tree_change_height(root, new_height, COMMIT_ATOMIC, &new_blockno);
			if (r < 0)
				return r;
			if (*prev_blockno != new_blockno)
			{
				root = (struct bpfs_tree_root*)
				           (get_block(new_blockno) + root_off);
				change_height_holes = true;
			}
		}

		if (int_valid < off)
		{
			r = truncate_block_zero(root, int_valid, off, int_valid,
			                        &new_blockno);
			if (r < 0)
				return r;
			if (*prev_blockno != new_blockno)
			{
				root = (struct bpfs_tree_root*)
				           (get_block(new_blockno) + root_off);
				change_height_holes = true;
			}
		}
	}

	child_new_blockno = tree_root_addr(root);
	max_nblocks = tree_max_nblocks(root->ha.height);
	if (commit != COMMIT_NONE)
	{
		child_size = size;
	}
	else
	{
		assert(end <= root->nbytes);
		child_size = MIN(size, max_nblocks * BPFS_BLOCK_SIZE - off);
	}
	child_valid = MIN(root->nbytes, max_nblocks * BPFS_BLOCK_SIZE);

	if (commit == COMMIT_NONE || commit == COMMIT_FREE || commit == COMMIT_COPY)
		child_commit = commit;
	else if (off < root->nbytes && root->nbytes < end)
		child_commit = COMMIT_COPY; // data needs atomic commit with nbytes
	else
		child_commit = commit;

	if (commit != COMMIT_NONE)
		xcall(indirect_cow_parent_push(new_blockno));
	if (!root->ha.height)
	{
		if (child_size)
			r = crawl_leaf(child_new_blockno, 0, off, child_size,
			               child_valid, off,
			               child_commit, callback, user, NULL,
			               &child_new_blockno);
		else
			r = 0;
	}
	else
	{
		r = crawl_indir(child_new_blockno, off / BPFS_BLOCK_SIZE,
		                off, child_size, child_valid,
                        off, child_commit, root->ha.height, max_nblocks,
		                callback, user, NULL, &child_new_blockno);
	}
	if (commit != COMMIT_NONE)
		indirect_cow_parent_pop(new_blockno);

	if (r >= 0)
	{
		bool change_addr = tree_root_addr(root) != child_new_blockno;
		bool change_size = end > root->nbytes;

		if (commit == COMMIT_NONE)
		{
			assert(!change_addr && !change_size);
			assert(*prev_blockno == new_blockno);
			if (r == 0)
				r = crawl_hole((off + child_size) / BPFS_BLOCK_SIZE,
				               child_size, size - child_size, root->nbytes,
				               off, callback, user);
		}
		else if (change_addr || change_size || change_height_holes)
		{
			bool overwrite = off < root->nbytes;
			bool inplace;

			// FYI:
			assert(!(!change_addr && overwrite && change_size));

#if COMMIT_MODE != MODE_BPFS
			assert(blockno_refed || block_freshly_alloced(*prev_blockno));
#endif

			if (*prev_blockno != new_blockno || !blockno_refed)
				inplace = true;
			else if (change_addr && overwrite && change_size)
				inplace = commit == COMMIT_FREE;
			else
			{
				inplace = commit == COMMIT_FREE;
#if COMMIT_MODE == MODE_BPFS
				static_assert(COMMIT_ATOMIC != COMMIT_COPY);
				inplace = inplace || commit == COMMIT_ATOMIC;
#endif
			}

			if (!inplace)
			{
				new_blockno = cow_block_entire(new_blockno);
				if (new_blockno == BPFS_BLOCKNO_INVALID)
					return -ENOSPC;
				if (change_size)
					indirect_cow_block_required(new_blockno);
				// else indirect_cow_block_required(new_blockno) not required
				root = (struct bpfs_tree_root*)
				           (get_block(new_blockno) + root_off);
			}

			if (change_addr)
			{
				ha_set_addr(&root->ha, child_new_blockno);
#if SCSP_OPT_APPEND
				if (!root->nbytes)
					indirect_cow_block_direct(new_blockno,
					                          block_offset(&root->ha),
					                          sizeof(root->ha));
#endif
			}
			if (change_size)
				root->nbytes = end;

			*prev_blockno = new_blockno;
		}
		else
		{
			assert(*prev_blockno == new_blockno);
		}
	}

	return r;
}

int crawl_tree(struct bpfs_tree_root *root, uint64_t off,
               uint64_t size, enum commit commit,
               crawl_callback callback, void *user,
               uint64_t *prev_blockno)
{
	return crawl_tree_ref(root, off, size, commit, callback, user, prev_blockno,
	                      true);
}

//
// crawl_inodes()

int crawl_inodes(uint64_t off, uint64_t size, enum commit commit,
                 crawl_callback callback, void *user)
{
	struct bpfs_tree_root *root = get_inode_root();
	struct bpfs_super *super = get_super();
	uint64_t super_blockno = get_super_blockno();
	uint64_t child_blockno = super->inode_root_addr;
	int r;

	if (commit != COMMIT_NONE)
		xcall(indirect_cow_parent_push(super_blockno));
	r = crawl_tree(root, off, size, commit, callback, user,
	               &child_blockno);
	if (commit != COMMIT_NONE)
		indirect_cow_parent_pop(super_blockno);

	if (r >= 0 && child_blockno != super->inode_root_addr)
	{
#if COMMIT_MODE == BPFS
		assert(commit == COMMIT_ATOMIC);
#else
		// COPY is ok because super points at a non-persistent block
		assert(commit == COMMIT_COPY || commit == COMMIT_ATOMIC);
#endif
#if COMMIT_MODE == MODE_SCSP
		assert(super_blockno != BPFS_BLOCKNO_SUPER);
#endif
		super->inode_root_addr = child_blockno;
	}

	return r;
}

//
// crawl_inode()

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

int crawl_inode(uint64_t ino, enum commit commit,
                crawl_callback_inode callback, void *user)
{
	struct callback_crawl_inode_data ccid = {callback, user};
	uint64_t ino_off;

	xcall(get_inode_offset(ino, &ino_off));

	return crawl_inodes(ino_off, sizeof(struct bpfs_inode), commit,
	                    callback_crawl_inode, &ccid);
}

//
// crawl_data()

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

int crawl_data(uint64_t ino, uint64_t off, uint64_t size,
               enum commit commit,
               crawl_callback callback, void *user)
{
	struct callback_crawl_data_data ccdd = {off, size, callback, user};

	return crawl_inode(ino, commit, callback_crawl_data, &ccdd);
}

//
// crawl_data_2()
// Crawl 2: atomically commit two non-contiguous writes

struct callback_crawl_data_2_data {
	struct ccd2dd {
		uint64_t ino;
		uint64_t ino_off;
		uint64_t off;
		uint64_t size;
		crawl_callback callback;
		void *user;
	} d[2];
};

static void ccd2dd_fill(struct ccd2dd *d,
                        uint64_t ino, uint64_t off, uint64_t size,
                        crawl_callback callback, void *user)
{
	d->ino = ino;
	xcall(get_inode_offset(ino, &d->ino_off));
	d->off = off;
	d->size = size;
	d->callback = callback;
	d->user = user;
}

static int callback_crawl_data_2_tree(uint64_t blockoff, char *block,
                                      unsigned off, unsigned size,
                                      unsigned valid, uint64_t crawl_start,
                                      enum commit commit, void *ccd2d_void,
                                      uint64_t *blockno)
{
	struct callback_crawl_data_2_data *ccd2d = (struct callback_crawl_data_2_data*) ccd2d_void;
	uint64_t prev_blockno = *blockno;
	uint64_t first_offset = blockoff * BPFS_BLOCK_SIZE + off;
	uint64_t last_offset = first_offset + size;
	unsigned mask, i;

	mask = first_offset == ccd2d->d[0].off;
	mask |= (last_offset == ccd2d->d[1].off + ccd2d->d[1].size) << 1;

	for (i = 1; i <= 2; i++)
	{
		if (i & mask)
		{
			struct ccd2dd *d = &ccd2d->d[i >> 1];
			bool new = *blockno != prev_blockno;
			enum commit c = new ? COMMIT_FREE : COMMIT_COPY;
			block = get_block(*blockno);
			int r = d->callback(blockoff, block, d->off % BPFS_BLOCK_SIZE,
			                    d->size, valid, crawl_start, c,
			                    d->user, blockno);
			if (r < 0)
			{
				assert(i == 1); // Need cleanup for i==2, but shouldn't happen
				return r;
			}
		}
	}

	return 0;
}

static int callback_crawl_data_2(uint64_t blockoff, char *block,
                                 unsigned off, unsigned size, unsigned valid,
                                 uint64_t crawl_start, enum commit commit,
                                 void *ccd2d_void, uint64_t *blockno)
{
	struct callback_crawl_data_2_data *ccd2d = (struct callback_crawl_data_2_data*) ccd2d_void;
	uint64_t first_offset = blockoff * BPFS_BLOCK_SIZE + off;
	uint64_t last_offset = first_offset + size - sizeof(struct bpfs_inode);
	unsigned mask;

	mask  = first_offset == ccd2d->d[0].ino_off;
	mask |= (last_offset == ccd2d->d[1].ino_off) << 1;

	if (mask == 3)
	{
		struct bpfs_inode *inode = (struct bpfs_inode*) (block + off);
		if (ccd2d->d[0].ino == ccd2d->d[1].ino)
		{
			assert(ccd2d->d[0].off < ccd2d->d[1].off);
			return crawl_tree(&inode->root, ccd2d->d[0].off,
			                  ccd2d->d[1].off - ccd2d->d[0].off
			                  + ccd2d->d[1].size, commit,
			                  callback_crawl_data_2_tree, ccd2d, blockno);
		}
		else
		{
#if COMMIT_MODE == MODE_BPFS && !defined(NDEBUG)
			uint64_t prev_blockno = *blockno;
#endif
			struct bpfs_inode *inode_1;
			int r;

			r = crawl_tree(&inode->root, ccd2d->d[0].off, ccd2d->d[0].size,
			               COMMIT_COPY,
			               ccd2d->d[0].callback, ccd2d->d[0].user,
			               blockno);
			if (r < 0)
				return r;
			block = get_block(*blockno);
			inode_1 =  (struct bpfs_inode*)
				(block + off + size - sizeof(struct bpfs_inode));

#if COMMIT_MODE == MODE_BPFS
			assert(prev_blockno != *blockno); // Required for !blockno_refed
#endif
			r = crawl_tree_ref(&inode_1->root, ccd2d->d[1].off,
			                   ccd2d->d[1].size, COMMIT_COPY,
			                   ccd2d->d[1].callback, ccd2d->d[1].user,
			                   blockno, false);
			assert(r >= 0); // FIXME: recover first crawl_tree() changes
			return r;
		}
	}
	else if (mask)
	{
		struct bpfs_inode *inode;
		struct ccd2dd *d = &ccd2d->d[mask >> 1];
		if (mask == 1)
			inode = (struct bpfs_inode*) (block + off);
		else
		{
			assert(mask == 2);
			inode = (struct bpfs_inode*)
				(block + off + size - sizeof(struct bpfs_inode));
		}
		assert(commit == COMMIT_COPY);
		return crawl_tree(&inode->root, d->off, d->size, commit,
		                  d->callback, d->user, blockno);
	}

	return 0;
}

#ifndef NDEBUG
static bool region_in_one_block(uint64_t off, uint64_t size)
{
	return (off % BPFS_BLOCK_SIZE) + size <= BPFS_BLOCK_SIZE;
}
#endif

int crawl_data_2(uint64_t ino_0, uint64_t off_0, uint64_t size_0,
                 crawl_callback callback_0, void *user_0,
                 uint64_t ino_1, uint64_t off_1, uint64_t size_1,
                 crawl_callback callback_1, void *user_1,
                 enum commit commit)
{
	struct callback_crawl_data_2_data ccd2d;
	uint64_t ino_start, ino_end, ino_size;
	unsigned idx_0, idx_1;

	// Overlap not allowed
	assert(!(ino_0 == ino_1
	         && ((off_0 <= off_1 && off_1 < off_0 + size_0)
	             || (off_1 <= off_0 && off_0 < off_1 + size_1))));
	// callback_crawl_data_2_tree() simplification:
	assert(region_in_one_block(off_0, size_0));
	assert(region_in_one_block(off_1, size_1));

	if (ino_0 < ino_1 || (ino_0 == ino_1 && off_0 <= off_1))
	{
		idx_0 = 0;
		idx_1 = 1;
	}
	else
	{
		idx_0 = 1;
		idx_1 = 0;
	}
	ccd2dd_fill(&ccd2d.d[idx_0], ino_0, off_0, size_0, callback_0, user_0);
	ccd2dd_fill(&ccd2d.d[idx_1], ino_1, off_1, size_1, callback_1, user_1);

	ino_start = ccd2d.d[0].ino_off;
	ino_end = ccd2d.d[1].ino_off + sizeof(struct bpfs_inode);
	ino_size = ino_end - ino_start;

	return crawl_inodes(ino_start, ino_size, commit,
	                    callback_crawl_data_2, &ccd2d);
}

//
// crawler_init()

void crawler_init(void)
{
	// linkers have maximum alignments:
	assert(!(((uintptr_t) zero_block) % sysconf(_SC_PAGE_SIZE)));
	// make sure mprotect() doesn't mark other data as read-only:
	assert(!(BPFS_BLOCK_SIZE % sysconf(_SC_PAGE_SIZE)));
	// make sure code does not write into the block of zeros:
	xsyscall(mprotect(zero_block, BPFS_BLOCK_SIZE, PROT_READ));
}
