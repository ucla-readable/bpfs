/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef BPFS_STRUCTS_H
#define BPFS_STRUCTS_H

#include "util.h"

#include <stdint.h>

#define BPFS_FS_MAGIC 0xB9F5

#define BPFS_STRUCT_VERSION 7

#define BPFS_BLOCK_SIZE 4096

#define BPFS_BLOCKNO_INVALID 0
#define BPFS_BLOCKNO_SUPER 1
#define BPFS_BLOCKNO_SUPER_2 2
#define BPFS_BLOCKNO_FIRST_ALLOC 3

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

#define __BPFS_S_ISTYPE(mode, mask) (((mode) & BPFS_S_IFMT) == (mask))
#define BPFS_S_ISSOCK(mode) __BPFS_S_ISTYPE((mode), BPFS_S_IFSOCK)
#define BPFS_S_ISLNK(mode)  __BPFS_S_ISTYPE((mode), BPFS_S_IFLNK)
#define BPFS_S_ISREG(mode)  __BPFS_S_ISTYPE((mode), BPFS_S_IFREG)
#define BPFS_S_ISBLK(mode)  __BPFS_S_ISTYPE((mode), BPFS_S_IFBLK)
#define BPFS_S_ISDIR(mode)  __BPFS_S_ISTYPE((mode), BPFS_S_IFDIR)
#define BPFS_S_ISCHR(mode)  __BPFS_S_ISTYPE((mode), BPFS_S_IFCHR)
#define BPFS_S_ISFIFO(mode) __BPFS_S_ISTYPE((mode), BPFS_S_IFIFO)

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

#define BPFS_TREE_LOG_MAX_HEIGHT 3
#define BPFS_TREE_MAX_HEIGHT ((((uint64_t) 1) << BPFS_TREE_LOG_MAX_HEIGHT) - 1)
#define BPFS_TREE_LOG_ROOT_MAX_ADDR (sizeof(uint64_t) * 8 - BPFS_TREE_LOG_MAX_HEIGHT)
#define BPFS_TREE_ROOT_MAX_ADDR ((((uint64_t) 1) << BPFS_TREE_LOG_ROOT_MAX_ADDR) - 1)

struct height_addr
{
	uint64_t height : BPFS_TREE_LOG_MAX_HEIGHT; // #levels of indir blocks
	uint64_t addr   : BPFS_TREE_LOG_ROOT_MAX_ADDR;
};

struct bpfs_tree_root
{
	struct height_addr ha; // valid iff !!nbytes
	uint64_t nbytes;
};

// bpfs_super.commit_mode options:
#define BPFS_COMMIT_SP 0
#define BPFS_COMMIT_SCSP 1

struct bpfs_super
{
	uint32_t magic;
	uint32_t version;
	uint8_t  uuid[16];
	uint64_t nblocks;
	uint64_t inode_root_addr; // block number containing the inode tree root
	uint64_t inode_root_addr_2; // only used with SP; for commit consistency
	uint8_t commit_mode;
	uint8_t ephemeral_valid; // for SCSP, inode link count validity
	uint8_t pad[4046]; // pad to full block
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
	uint32_t uid;
	uint32_t gid;
	uint32_t mode;
	uint32_t nlinks; // valid at mount iff bpfs_super.ephemeral_valid
	uint64_t flags;
	struct bpfs_tree_root root;
	struct bpfs_time atime;
	struct bpfs_time ctime;
	struct bpfs_time mtime;
	uint8_t pad[68]; // pad to evenly fill a block
};

#define BPFS_INODES_PER_BLOCK (BPFS_BLOCK_SIZE / sizeof(struct bpfs_inode))


struct bpfs_dirent
{
	uint64_t ino;
	uint16_t rec_len;
	uint8_t file_type;
	uint8_t name_len;
	char name[];
} __attribute__((packed)); // pack rather than manually pad for char name[]

#define BPFS_DIRENT_ALIGN 8
#define BPFS_DIRENT_MAX_NAME_LEN \
	MIN(BPFS_BLOCK_SIZE - sizeof(struct bpfs_dirent), \
	    1 << (sizeof(((struct bpfs_dirent*) NULL)->name_len) * 8))
#define BPFS_DIRENT_LEN(name_len) \
	ROUNDUP64(sizeof(struct bpfs_dirent) + (name_len), BPFS_DIRENT_ALIGN)
#define BPFS_DIRENT_MIN_LEN BPFS_DIRENT_LEN(0)


// static_assert() must be used in a function, so declare one solely for this
// purpose. It returns its own address to avoid an unused function warning.
static inline void* __bpfs_structs_static_asserts(void)
{
	static_assert(sizeof(struct height_addr) == 8); // need to set atomically
	static_assert(!(sizeof(struct bpfs_tree_root) % 8));
	static_assert(sizeof(struct bpfs_super) == BPFS_BLOCK_SIZE);
	static_assert(sizeof(struct bpfs_indir_block) == BPFS_BLOCK_SIZE);
	static_assert(sizeof(struct bpfs_time) == 4);
	static_assert(sizeof(struct bpfs_inode) == 128); // fit evenly in a block
	// struct bpfs_dirent itself does not have alignment restrictions
	static_assert(sizeof(struct bpfs_dirent) == 12);
	static_assert(!(BPFS_DIRENT_MIN_LEN % 8));
	return __bpfs_structs_static_asserts;
}

#endif
