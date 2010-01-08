#include "bpfs_structs.h"
#include "util.h"

#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <uuid/uuid.h>

#define BPFS_MIN_NBYTES ((uint64_t) 3 * BPFS_BLOCK_SIZE)

static char *bpram;
static size_t bpram_size;
static struct bpfs_super *super;

static char* get_block(uint64_t no)
{
	assert(no != BPFS_BLOCKNO_INVALID);
	assert(no <= super->nblocks);
	return bpram + (no - 1) * BPFS_BLOCK_SIZE;
}

static void mkfs(void)
{
	struct bpfs_inode *inodes;
	struct bpfs_inode *root_inode;
	struct bpfs_dirent *root_dirent;
	time_t now;

	if (bpram_size < BPFS_MIN_NBYTES)
	{
		fprintf(stderr, "BPFS requires at least %" PRIu64 " bytes of BPRAM.\n",
		        BPFS_MIN_NBYTES);
		exit(1);
	}

	time(&now);

	super = (struct bpfs_super*) bpram;
	super->magic = BPFS_FS_MAGIC;
	super->version = 1;
	static_assert(sizeof(uuid_t) == sizeof(super->uuid));
	uuid_generate(super->uuid);
	super->nblocks = bpram_size / BPFS_BLOCK_SIZE;
	super->bitmap_addr = BPFS_BLOCKNO_INVALID;
	super->inode_addr = 2;
	super->ninodeblocks = 1;

	inodes = (struct bpfs_inode*) get_block(super->inode_addr);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	memset(inodes, 0, super->ninodeblocks * BPFS_BLOCK_SIZE);

	root_inode = &inodes[0];
	root_inode->generation = 1;
	root_inode->mode = BPFS_S_IFDIR;
	root_inode->mode |= BPFS_S_IRUSR | BPFS_S_IWUSR | BPFS_S_IXUSR | BPFS_S_IRGRP | BPFS_S_IWGRP | BPFS_S_IXGRP | BPFS_S_IROTH | BPFS_S_IXOTH;
	root_inode->uid = 0;
	root_inode->gid = 0;
	root_inode->nlinks = 2;
	root_inode->nbytes = BPFS_BLOCK_SIZE;
	root_inode->nblocks = 1;
	root_inode->atime.sec = (typeof(root_inode->atime.sec)) now;
	xassert(root_inode->atime.sec == now);
	root_inode->mtime = root_inode->ctime = root_inode->atime;
	// TODO: flags
	root_inode->block_addr = super->inode_addr + super->ninodeblocks;

	root_dirent = (struct bpfs_dirent*) get_block(root_inode->block_addr);
	static_assert(BPFS_INO_INVALID == 0);
	memset(root_dirent, 0, BPFS_BLOCK_SIZE);
	root_dirent->ino = BPFS_INO_ROOT;
	root_dirent->file_type = BPFS_TYPE_DIR;
	strcpy(root_dirent->name, "..");
	root_dirent->name_len = strlen(root_dirent->name) + 1;
	root_dirent->rec_len = BPFS_DIRENT_LEN(root_dirent->name_len);
}

int main(int argc, char **argv)
{
	char *bpram_name;
	int bpram_fd;
	struct stat stbuf;

	if (argc != 2)
	{
		fprintf(stderr, "Usage: %s <bpram_device>\n", argv[0]);
		exit(1);
	}

	bpram_name = argv[1];

	bpram_fd = xsyscall(open(bpram_name, O_RDWR));
	
	xsyscall(fstat(bpram_fd, &stbuf));
	bpram_size = stbuf.st_size;
	xassert(bpram_size == stbuf.st_size);

	xassert(!(bpram_size % BPFS_BLOCK_SIZE));

	bpram = mmap(NULL, bpram_size, PROT_READ | PROT_WRITE, MAP_SHARED, bpram_fd, 0);
	xassert(bpram);

	mkfs();

	xsyscall(msync(bpram, bpram_size, MS_SYNC));
	xsyscall(munmap(bpram, bpram_size));
	xsyscall(close(bpram_fd));

	return 0;
}
