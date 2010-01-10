#ifndef BPFS_STRUCTS_H
#define BPFS_STRUCTS_H

#include <stdint.h>

#define BPFS_FS_MAGIC 0xB9F5

#define BPFS_BLOCK_SIZE 4096

#define BPFS_BLOCKNO_INVALID 0

#define BPFS_INO_INVALID 0
#define BPFS_INO_ROOT    1

#define BPFS_S_IFMT   0xF000
#define BPFS_S_IFSOCK 0xC000
#define BPFS_S_IFLNK  0xA000
#define BPFS_S_IFREG  0x8000
#define BPFS_S_IFBLK  0x6000
#define BPFS_S_IFDIR  0x4000
#define BPFS_S_IFCHR  0x2000
#define BPFS_S_IFIFO  0x1000

#define BPFS_S_IPERM  0x0FFF
#define BPFS_S_ISUID  0x0800  // SUID
#define BPFS_S_ISGID  0x0400  // SGID
#define BPFS_S_ISVTX  0x0200  // sticky bit
#define BPFS_S_IRWXU  0x01C0  // user access rights mask
#define BPFS_S_IRUSR  0x0100  // read
#define BPFS_S_IWUSR  0x0080  // write
#define BPFS_S_IXUSR  0x0040  // execute
#define BPFS_S_IRWXG  0x0038  // group access rights mask
#define BPFS_S_IRGRP  0x0020  // read
#define BPFS_S_IWGRP  0x0010  // write
#define BPFS_S_IXGRP  0x0008  // execute
#define BPFS_S_IRWXO  0x0007  // others access rights mask
#define BPFS_S_IROTH  0x0004  // read
#define BPFS_S_IWOTH  0x0002  // write
#define BPFS_S_IXOTH  0x0001  // execute

#define BPFS_TYPE_UNKNOWN 0
#define BPFS_TYPE_FILE    1
#define BPFS_TYPE_DIR     2
#define BPFS_TYPE_CHRDEV  3
#define BPFS_TYPE_BLKDEV  4
#define BPFS_TYPE_FIFO    5
#define BPFS_TYPE_SOCK    6
#define BPFS_TYPE_SYMLINK 7


struct bpfs_super
{
	uint32_t magic;
	uint32_t version;
	uint8_t  uuid[16];
	uint64_t nblocks;
//	struct bpfs_tree_root inode_root; // TODO
	uint64_t inode_addr; // just the second block?
	uint64_t ninodeblocks;
};


#define BPFS_TREE_MAX_HEIGHT 5
#define BPFS_TREE_ROOT_MAX_ADDR (1 << (sizeof(uint64_t) * 8 - BPFS_TREE_MAX_HEIGHT))

struct bpfs_tree_root
{
	uint64_t height; // : BPFS_TREE_MAX_HEIGHT; // #levels of indir blocks
	uint64_t addr; // : sizeof(uint64_t) * 8 - BPFS_TREE_MAX_HEIGHT;
	uint64_t nbytes;
	uint64_t nblocks; // TODO: remove, to update size atomically and in-place
};


#define BPFS_BLOCKNOS_PER_INDIR (BPFS_BLOCK_SIZE / sizeof(uint64_t))

struct bpfs_indir_block
{
	uint64_t addr[BPFS_BLOCKNOS_PER_INDIR];
};


struct bpfs_time
{
	uint32_t sec;
//	uint32_t ns;
};

struct bpfs_inode
{
	uint64_t generation;
	uint32_t mode;
	uint32_t uid;
	uint32_t gid;
	uint32_t nlinks;
	struct bpfs_time atime;
	struct bpfs_time ctime;
	struct bpfs_time mtime;
	uint64_t flags;
	struct bpfs_tree_root root;
};

#define BPFS_INODES_PER_BLOCK (BPFS_BLOCK_SIZE / sizeof(struct bpfs_inode))


struct bpfs_dirent
{
	uint64_t ino;
	uint16_t rec_len;
	uint8_t file_type;
	uint8_t name_len;
	char name[];
};

#define BPFS_DIRENT_ALIGN 8
#define BPFS_DIRENT_MAX_NAME_LEN (BPFS_BLOCK_SIZE - sizeof(struct bpfs_dirent) - 1)

#endif
