/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "mkbpfs.h"
#include "bpfs.h"
#include "bpfs_structs.h"
#include "util.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>

#define BPFS_MIN_NBLOCKS 7

// Appease users of bitmap_scan_t:
// The number of blocks must be a multiple of this number:
#define NBLOCKS_MODULUS (sizeof(bitmap_scan_t) * 8)
// The initial number of inode blocks:
#define INODES_NBLOCKS \
	CMAX(1, ROUNDUP64(sizeof(bitmap_scan_t) * 8, BPFS_INODES_PER_BLOCK) \
	        / BPFS_INODES_PER_BLOCK)

/* As of commit max(commits of this comment), mkbpfs() allocates these blocks:
 * 1: super
 * 2: super2
 * 3: inode root
 * 4: ir.indirect
 * 5: ir.data[0]
 * 6: ir.data[1]
 * 7: "/".data[0]
 */

static char* mk_get_block(char *bpram, struct bpfs_super *super, uint64_t no)
{
	assert(no != BPFS_BLOCKNO_INVALID);
	assert(no <= super->nblocks);
	assert(no <= BPFS_MIN_NBLOCKS);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return bpram + (no - 1) * BPFS_BLOCK_SIZE;
}

static uint64_t mk_alloc_block(struct bpfs_super *super)
{
	static uint64_t next_blockno = BPFS_BLOCKNO_FIRST_ALLOC - 1;
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	assert(next_blockno < super->nblocks);
	assert(next_blockno < BPFS_MIN_NBLOCKS);
	return (next_blockno++) + 1;
}

#define MK_GET_BLOCK(blockno) mk_get_block(bpram, super, blockno)

int mkbpfs(char *bpram, size_t bpram_size)
{
	struct bpfs_super *super;
	struct bpfs_super *super_2;
	struct bpfs_tree_root *inodes_root;
	struct bpfs_indir_block *inodes_indir;
	struct bpfs_inode *inodes;
	struct bpfs_inode *root_inode;
	struct bpfs_dirent *root_dirent;
	int i;

	if (bpram_size < BPFS_MIN_NBLOCKS * BPFS_BLOCK_SIZE)
		return -ENOSPC;
	if (bpram_size < NBLOCKS_MODULUS * BPFS_BLOCK_SIZE)
		return -ENOSPC;

	super = (struct bpfs_super*) bpram;
	super->version = BPFS_STRUCT_VERSION;
	static_assert(sizeof(uuid_t) == sizeof(super->uuid));
	uuid_generate(super->uuid);
	super->nblocks = ROUNDDOWN64(bpram_size / BPFS_BLOCK_SIZE, NBLOCKS_MODULUS);
	super->inode_root_addr = mk_alloc_block(super);
	super->inode_root_addr_2 = super->inode_root_addr; // not required for SCSP
	super->commit_mode = BPFS_COMMIT_SCSP;
	super->ephemeral_valid = 1;
	memset(super->pad, 0, sizeof(super->pad));

	if (super->nblocks > BPFS_TREE_ROOT_MAX_ADDR + 1)
	{
		// This simplifies block allocation: limiting nblocks to
		// BPFS_TREE_ROOT_MAX_ADDR means allocation code doesn't have
		// to ensure that tree root block numbers do not exceed this limit.
		fprintf(stderr, "%s: Limiting file system to %" PRIu64 " blocks (%"
		        PRIu64 " are available)\n", __FUNCTION__,
		        BPFS_TREE_ROOT_MAX_ADDR, super->nblocks);
		super->nblocks = BPFS_TREE_ROOT_MAX_ADDR + 1;
	}

	super_2 = super + 1;
	*super_2 = *super; // not required for SCSP

	inodes_root = (struct bpfs_tree_root*) MK_GET_BLOCK(super->inode_root_addr);
	static_assert(INODES_NBLOCKS <= BPFS_BLOCKNOS_PER_INDIR);
	inodes_root->ha.height = 1;
	inodes_root->ha.addr = mk_alloc_block(super);
	inodes_root->nbytes = INODES_NBLOCKS * BPFS_BLOCK_SIZE;

	inodes_indir = (struct bpfs_indir_block*) MK_GET_BLOCK(inodes_root->ha.addr);

	for (i = 0; i < INODES_NBLOCKS; i++)
	{
#if APPEASE_VALGRIND || DETECT_ZEROLINKS_WITH_LINKS
		int j;
#endif
		inodes_indir->addr[i] = mk_alloc_block(super);
		inodes = (struct bpfs_inode*) MK_GET_BLOCK(inodes_indir->addr[i]);

#if APPEASE_VALGRIND || DETECT_ZEROLINKS_WITH_LINKS
		for (j = 0; j + sizeof(struct bpfs_inode) <= BPFS_BLOCK_SIZE; j += sizeof(struct bpfs_inode))
		{
# if APPEASE_VALGRIND
			// init the generation field. not required, but appeases valgrind.
			inodes[j].generation = 0;
# endif
# if DETECT_ZEROLINKS_WITH_LINKS
			inodes[j].nlinks = 0;
# endif
		}
#endif
	}

	inodes = (struct bpfs_inode*) MK_GET_BLOCK(inodes_indir->addr[0]);

	root_inode = &inodes[0];
	root_inode->generation = 1;
	root_inode->mode = BPFS_S_IFDIR;
	root_inode->mode |= BPFS_S_IRUSR | BPFS_S_IWUSR | BPFS_S_IXUSR | BPFS_S_IRGRP | BPFS_S_IWGRP | BPFS_S_IXGRP | BPFS_S_IROTH | BPFS_S_IXOTH;
	root_inode->uid = 0;
	root_inode->gid = 0;
	root_inode->nlinks = 2;
	root_inode->flags = 0;
	root_inode->root.ha.height = 0;
	root_inode->root.ha.addr = mk_alloc_block(super);
	root_inode->root.nbytes = BPFS_BLOCK_SIZE;
	root_inode->mtime = root_inode->ctime = root_inode->atime = BPFS_TIME_NOW();
	memset(root_inode->pad, 0, sizeof(root_inode->pad));

	root_dirent = (struct bpfs_dirent*) MK_GET_BLOCK(root_inode->root.ha.addr);
	root_dirent->rec_len = 0;

	super->magic = BPFS_FS_MAGIC;
	super_2->magic = BPFS_FS_MAGIC;

	return 0;
}
