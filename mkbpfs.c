#include "mkbpfs.h"
#include "bpfs_structs.h"
#include "util.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>

#define BPFS_MIN_NBLOCKS 5

static char* get_block(char *bpram, struct bpfs_super *super, uint64_t no)
{
	assert(no != BPFS_BLOCKNO_INVALID);
	assert(no <= super->nblocks);
	assert(no <= BPFS_MIN_NBLOCKS);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	return bpram + (no - 1) * BPFS_BLOCK_SIZE;
}

static uint64_t alloc_block(struct bpfs_super *super)
{
	static uint64_t next_blockno = 2; /* 0 and 1 are superblocks */
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	assert(next_blockno < super->nblocks);
	return (next_blockno++) + 1;
}

#define GET_BLOCK(blockno) get_block(bpram, super, blockno)

int mkbpfs(char *bpram, size_t bpram_size)
{
	struct bpfs_super *super;
	struct bpfs_super *super_2;
	struct bpfs_tree_root *inodes_root;
	struct bpfs_inode *inodes;
	struct bpfs_inode *root_inode;
	struct bpfs_dirent *root_dirent;

	if (bpram_size < BPFS_MIN_NBLOCKS * BPFS_BLOCK_SIZE)
		return -ENOSPC;

	super = (struct bpfs_super*) bpram;
#ifndef NDEBUG
	super->magic = 0; // set after file system is created. init for valgrind.
#endif
	super->version = BPFS_STRUCT_VERSION;
	static_assert(sizeof(uuid_t) == sizeof(super->uuid));
	uuid_generate(super->uuid);
	super->nblocks = bpram_size / BPFS_BLOCK_SIZE;
	super->inode_root_addr = alloc_block(super);
	super->inode_root_addr_2 = super->inode_root_addr; // not required for SCSP
	super->commit_mode = BPFS_COMMIT_SCSP;
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

	inodes_root = (struct bpfs_tree_root*) get_block(bpram, super, super->inode_root_addr);
	inodes_root->ha.height = 0;
	inodes_root->ha.addr = alloc_block(super);
	inodes_root->nbytes = BPFS_BLOCK_SIZE;

	inodes = (struct bpfs_inode*) GET_BLOCK(inodes_root->ha.addr);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
#ifndef NDEBUG
	{
		// init the generation field. not required, but appeases valgrind.
		int i;
		for (i = 0; i + sizeof(struct bpfs_inode) <= BPFS_BLOCK_SIZE; i += sizeof(struct bpfs_inode))
			inodes[i].generation = 0;
	}
#endif
	static_assert(BPFS_INO_INVALID == 0);
	memset(inodes, 0, inodes_root->nbytes);

	root_inode = &inodes[0];
	root_inode->generation = 1;
	root_inode->mode = BPFS_S_IFDIR;
	root_inode->mode |= BPFS_S_IRUSR | BPFS_S_IWUSR | BPFS_S_IXUSR | BPFS_S_IRGRP | BPFS_S_IWGRP | BPFS_S_IXGRP | BPFS_S_IROTH | BPFS_S_IXOTH;
	root_inode->uid = 0;
	root_inode->gid = 0;
	root_inode->nlinks = 2;
	root_inode->flags = 0;
	root_inode->root.ha.height = 0;
	root_inode->root.ha.addr = alloc_block(super);
	root_inode->root.nbytes = BPFS_BLOCK_SIZE;
	root_inode->mtime = root_inode->ctime = root_inode->atime = BPFS_TIME_NOW();
	memset(root_inode->pad, 0, sizeof(root_inode->pad));

	root_dirent = (struct bpfs_dirent*) GET_BLOCK(root_inode->root.ha.addr);
	static_assert(BPFS_INO_INVALID == 0);
	memset(root_dirent, 0, BPFS_BLOCK_SIZE);
	root_dirent->ino = BPFS_INO_ROOT;
	root_dirent->file_type = BPFS_TYPE_DIR;
	strcpy(root_dirent->name, "..");
	root_dirent->name_len = strlen(root_dirent->name) + 1;
	root_dirent->rec_len = BPFS_DIRENT_LEN(root_dirent->name_len);

	super->magic = BPFS_FS_MAGIC;
	super_2->magic = BPFS_FS_MAGIC;

	return 0;
}
