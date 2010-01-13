#include "mkbpfs.h"
#include "bpfs_structs.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <uuid/uuid.h>

#define BPFS_MIN_NBYTES ((uint64_t) 3 * BPFS_BLOCK_SIZE)

static char* get_block(char *bpram, struct bpfs_super *super, uint64_t no)
{
	assert(no != BPFS_BLOCKNO_INVALID);
	assert(no <= super->nblocks);
	return bpram + (no - 1) * BPFS_BLOCK_SIZE;
}

#define GET_BLOCK(blockno) get_block(bpram, super, blockno)

int mkbpfs(char *bpram, size_t bpram_size)
{
	struct bpfs_super *super;
	struct bpfs_inode *inodes;
	struct bpfs_inode *root_inode;
	struct bpfs_dirent *root_dirent;
	int i;

	if (bpram_size < BPFS_MIN_NBYTES)
		return -ENOSPC;

	super = (struct bpfs_super*) bpram;
	super->magic = BPFS_FS_MAGIC;
	super->version = 1;
	static_assert(sizeof(uuid_t) == sizeof(super->uuid));
	uuid_generate(super->uuid);
	super->nblocks = bpram_size / BPFS_BLOCK_SIZE;
	super->inode_root.addr = 2;
	super->inode_root.height = 0;
	super->inode_root.nblocks = 1;
	super->inode_root.nbytes = super->inode_root.nblocks * BPFS_BLOCK_SIZE;

	inodes = (struct bpfs_inode*) GET_BLOCK(super->inode_root.addr);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
#ifndef NDEBUG
	// init the generation field. not required, but appeases valgrind.
	for (i = 0; i + sizeof(struct bpfs_inode) <= BPFS_BLOCK_SIZE; i += sizeof(struct bpfs_inode))
		inodes[i].generation = 0;
#endif
	memset(inodes, 0, super->inode_root.nblocks * BPFS_BLOCK_SIZE);

	root_inode = &inodes[0];
	root_inode->generation = 1;
	root_inode->mode = BPFS_S_IFDIR;
	root_inode->mode |= BPFS_S_IRUSR | BPFS_S_IWUSR | BPFS_S_IXUSR | BPFS_S_IRGRP | BPFS_S_IWGRP | BPFS_S_IXGRP | BPFS_S_IROTH | BPFS_S_IXOTH;
	root_inode->uid = 0;
	root_inode->gid = 0;
	root_inode->nlinks = 2;
	root_inode->root.height = 0;
	root_inode->root.nbytes = BPFS_BLOCK_SIZE;
	root_inode->root.nblocks = 1;
	root_inode->mtime = root_inode->ctime = root_inode->atime = BPFS_TIME_NOW();
	// TODO: flags
	root_inode->root.addr = super->inode_root.addr + super->inode_root.nblocks;

	root_dirent = (struct bpfs_dirent*) GET_BLOCK(root_inode->root.addr);
	static_assert(BPFS_INO_INVALID == 0);
	memset(root_dirent, 0, BPFS_BLOCK_SIZE);
	root_dirent->ino = BPFS_INO_ROOT;
	root_dirent->file_type = BPFS_TYPE_DIR;
	strcpy(root_dirent->name, "..");
	root_dirent->name_len = strlen(root_dirent->name) + 1;
	root_dirent->rec_len = BPFS_DIRENT_LEN(root_dirent->name_len);

	return 0;
}
