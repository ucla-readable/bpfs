#include "mkbpfs.h"
#include "bpfs_structs.h"
#include "util.h"

#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>

#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <time.h>
#include <unistd.h>

// TODO:
// - make time higher resolution. See ext4, bits/stat.h, linux/time.h.
// - add gcc flag to warn about ignored return values?


// STDTIMEOUT is not 0 because of a fuse kernel module bug.
// Miklos's 2006/06/27 email, E1FvBX0-0006PB-00@dorka.pomaz.szeredi.hu, fixes.
#define STDTIMEOUT 1.0

#define FUSE_ERR_SUCCESS 0

#define DEBUG 1
#if DEBUG
# define Dprintf(x...) fprintf(stderr, x)
#else
# define Dprintf(x...) do {} while(0)
#endif


static char *bpram;
static size_t bpram_size;

static struct bpfs_super *super;


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

struct bitmap {
	char *bitmap;
	uint64_t ntotal;
	uint64_t nfree;
};

static int bitmap_init(struct bitmap *bitmap, uint64_t ntotal)
{
	size_t size = ROUNDUP64(ntotal, 8) / 8;
	assert(!bitmap->bitmap);
	bitmap->bitmap = malloc(size);
	if (!bitmap->bitmap)
		return -ENOMEM;
	memset(bitmap->bitmap, 0, size);
	bitmap->nfree = bitmap->ntotal = ntotal;
	return 0;
}

static void bitmap_destroy(struct bitmap *bitmap)
{
	free(bitmap->bitmap);
	bitmap->bitmap = NULL;
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
					*word |= 1 << j;
					bitmap->nfree--;
					return i + j;
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

static void bitmap_free(struct bitmap *bitmap, uint64_t no)
{
	char *word = bitmap->bitmap + no / 8;
	assert(no < bitmap->ntotal);
	assert(*word & (1 << (no % 8)));
	*word &= ~(1 << (no % 8));
	bitmap->nfree++;
}


//
// block allocation

static struct bitmap block_bitmap;

static int init_block_allocations(void)
{
	return bitmap_init(&block_bitmap, super->nblocks);
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
	return no + 1;
}

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
}

static char* get_block(uint64_t blockno)
{
	if (blockno == BPFS_BLOCKNO_INVALID)
	{
		assert(0);
		return NULL;
	}
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	if (blockno > super->nblocks)
	{
		assert(0);
		return NULL;
	}
	return bpram + (blockno - 1) * BPFS_BLOCK_SIZE;
}


//
// inode allocation

static struct bitmap inode_bitmap;

static int init_inode_allocations(void)
{
	return bitmap_init(&inode_bitmap, super->ninodeblocks * BPFS_INODES_PER_BLOCK);
}

static void destroy_inode_allocations(void)
{
	bitmap_destroy(&inode_bitmap);
}

static uint64_t alloc_inode(void)
{
	uint64_t no = bitmap_alloc(&inode_bitmap);
	if (no == inode_bitmap.ntotal)
		return BPFS_INO_INVALID;
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

static struct bpfs_inode* get_inode(uint64_t ino)
{
	uint64_t no;
	struct bpfs_inode *inodes;
	if (ino == BPFS_INO_INVALID)
	{
		assert(0);
		return NULL;
	}
	static_assert(BPFS_INO_INVALID == 0);
	no = ino - 1;
	if (no >= inode_bitmap.ntotal)
	{
		assert(0);
		return NULL;
	}
	inodes = (struct bpfs_inode*) get_block(super->inode_addr);
	return &inodes[no];
}


//
// block and inode allocations

static void discover_allocations(uint64_t ino)
{
	struct bpfs_inode *inode = get_inode(ino);
	static_assert(BPFS_INO_INVALID == 0);
	static_assert(BPFS_BLOCKNO_INVALID == 0);
	xassert(inode);

	xassert(ino <= inode_bitmap.ntotal);
	set_inode(ino);
	if (inode->block_addr != BPFS_BLOCKNO_INVALID)
	{
		xassert(inode->block_addr <= block_bitmap.ntotal);
		set_block(inode->block_addr);
	}

	if (inode->mode & BPFS_S_IFDIR)
	{
		uint64_t off = 0;
		while (off < inode->nbytes)
		{
			struct bpfs_dirent *dirent = (struct bpfs_dirent*) (get_block(inode->block_addr + off / BPFS_BLOCK_SIZE) + off % BPFS_BLOCK_SIZE);
			if (!dirent->rec_len)
			{
				// end of directory entries
				break;
			}
			off += dirent->rec_len;
			if (dirent->ino == BPFS_INO_INVALID)
				continue;
			if (!strcmp(dirent->name, ".."))
				continue;
			xassert(dirent->ino <= block_bitmap.ntotal);
			discover_allocations(dirent->ino);
		}
	}
}

static int init_allocations(void)
{
	uint64_t i;
	xcall(init_block_allocations());
	xcall(init_inode_allocations());
	bitmap_set(&block_bitmap, 0); // superblock
	for (i = 0; i < super->ninodeblocks; i++)
		bitmap_set(&block_bitmap, 1 + i);
	discover_allocations(BPFS_INO_ROOT);
	return 0;
}

static void destroy_allocations(void)
{
	destroy_inode_allocations();
	destroy_block_allocations();
}


//
// internal functions

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
	stbuf->st_size = inode->nbytes;
	stbuf->st_blocks = inode->nblocks * BPFS_BLOCK_SIZE / 512;
	stbuf->st_atime = inode->atime.sec;
	stbuf->st_mtime = inode->mtime.sec;
	stbuf->st_ctime = inode->ctime.sec;
	return 0;
}


static struct bpfs_dirent* find_dirent(struct bpfs_inode *parent,
                                       const char *name, size_t name_len)
{
	uint64_t off = 0;
	struct bpfs_dirent *dirent;

	assert(parent->mode & BPFS_S_IFDIR);
	assert(parent->block_addr != BPFS_BLOCKNO_INVALID);

	while (off < parent->nbytes)
	{
		dirent = (struct bpfs_dirent*) (get_block(parent->block_addr + off / BPFS_BLOCK_SIZE) + off % BPFS_BLOCK_SIZE);
		if (!dirent->rec_len)
		{
			// end of directory entries
			break;
		}
		off += dirent->rec_len;
		if (dirent->ino == BPFS_INO_INVALID)
			continue;
		if (name_len == dirent->name_len && !strcmp(name, dirent->name))
		{
			return dirent;
		}
	}
	return NULL;
}

static int alloc_dirent(struct bpfs_inode *parent, const char *name,
                        size_t name_len, struct bpfs_dirent **pdirent)
{
	struct bpfs_dirent *dirent;
	uint64_t off = 0;
	char *dirblock;

	// TODO: multiple blocks
	dirblock = get_block(parent->block_addr + off / BPFS_BLOCK_SIZE);
	while (1)
	{
		if (off + sizeof(*dirent) + name_len > parent->nbytes)
		{
			// TODO: alloc space
			return -ENOSPC;
		}

		dirent = (struct bpfs_dirent*) (dirblock + off % BPFS_BLOCK_SIZE);
		if (!dirent->rec_len)
		{
			// end of directory entries
			break;
		}
		if (dirent->ino == BPFS_INO_INVALID && dirent->rec_len >= sizeof(*dirent) + name_len)
		{
			// empty dirent
			break;
		}
		off += dirent->rec_len;
	}

	// Caller sets dirent->ino and dirent->file_type
	if (!dirent->rec_len)
		dirent->rec_len = BPFS_DIRENT_LEN(name_len);
	dirent->name_len = name_len;
	memcpy(dirent->name, name, name_len);

	*pdirent = dirent;
	return 0;
}

static int create_file(fuse_req_t req, fuse_ino_t parent_ino,
                       const char *name, mode_t mode,
                       struct bpfs_dirent **pdirent)
{
	struct bpfs_inode *parent = get_inode(parent_ino);
	size_t name_len = strlen(name) + 1;
	uint64_t ino;
	struct bpfs_inode *inode;
	struct bpfs_dirent *dirent;
	const struct fuse_ctx *ctx;
	time_t now;
	int r;

	if (name_len > BPFS_DIRENT_MAX_NAME_LEN)
		return -ENAMETOOLONG;

	if (!parent)
		return -ENOENT;

	ino = alloc_inode();
	if (ino == BPFS_INO_INVALID)
		return -ENOSPC;
	inode = get_inode(ino);
	assert(inode);

	dirent = find_dirent(parent, name, name_len);
	if (dirent)
		return -EEXIST;

	if ((r = alloc_dirent(parent, name, name_len, &dirent)) < 0)
		return r;

	ctx = fuse_req_ctx(req);

	time(&now);

	inode->generation++;
	assert(inode->generation); // not allowed to repeat within a fuse session
	inode->mode = f2b_mode(mode);
	inode->uid = ctx->uid;
	inode->gid = ctx->gid;
	inode->nlinks = 1;
	inode->nblocks = 0;
	inode->atime.sec = (typeof(inode->atime.sec)) now;
	xassert(inode->atime.sec == now);
	inode->mtime = inode->ctime = inode->atime;
	// TODO: flags
	inode->block_addr = BPFS_BLOCKNO_INVALID;

	if (mode & S_IFDIR)
	{
		struct bpfs_dirent *ndirent;

		inode->block_addr = alloc_block();
		if (inode->block_addr == BPFS_BLOCKNO_INVALID)
			return -ENOSPC;
		inode->nblocks++;
		inode->nbytes += BPFS_BLOCK_SIZE;
		ndirent = (struct bpfs_dirent*) get_block(inode->block_addr);
		assert(ndirent);

		inode->nlinks++;

		parent->nlinks++;
		static_assert(BPFS_INO_INVALID == 0);
		memset(ndirent, 0, BPFS_BLOCK_SIZE);
		ndirent->ino = parent_ino;
		ndirent->file_type = BPFS_TYPE_DIR;
		strcpy(ndirent->name, "..");
		ndirent->name_len = strlen(ndirent->name) + 1;
		ndirent->rec_len = BPFS_DIRENT_LEN(ndirent->name_len);
	}

	dirent->ino = ino;
	dirent->file_type = f2b_filetype(mode);

	*pdirent = dirent;

	return 0;
}

//
// fuse interface

static void fuse_init(void *userdata, struct fuse_conn_info *conn)
{
	static_assert(FUSE_ROOT_ID == BPFS_INO_ROOT);
	Dprintf("%s()\n", __FUNCTION__);
}

static void fuse_destroy(void *userdata)
{
	Dprintf("%s()\n", __FUNCTION__);
}


static void fuse_statfs(fuse_req_t req, fuse_ino_t ino)
{
	struct bpfs_inode *inode = get_inode(ino);
	struct statvfs stv;
	UNUSED(ino);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	if (!inode)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	stv.f_bsize = BPFS_BLOCK_SIZE;
	stv.f_frsize = BPFS_BLOCK_SIZE;
	static_assert(sizeof(stv.f_blocks) >= sizeof(super->nblocks));
	stv.f_blocks = super->nblocks;
	stv.f_bfree = block_bitmap.nfree;
	stv.f_bavail = stv.f_bfree; // NOTE: no space reserved for root
	stv.f_files = inode_bitmap.ntotal - inode_bitmap.nfree;
	stv.f_ffree = inode_bitmap.nfree;
	stv.f_favail = stv.f_ffree; // NOTE: no space reserved for root
	memset(&stv.f_fsid, 0, sizeof(stv.f_fsid)); // TODO: good enough?
	stv.f_flag = 0; // TODO: check for flags (see mount(8))
	stv.f_namemax = BPFS_DIRENT_MAX_NAME_LEN;
	xcall(fuse_reply_statfs(req, &stv));
}

static void fill_fuse_entry(const struct bpfs_dirent *dirent, struct fuse_entry_param *e)
{
	memset(e, 0, sizeof(e));
	e->ino = dirent->ino;
	e->attr_timeout = STDTIMEOUT;
	e->entry_timeout = STDTIMEOUT;
	xcall(bpfs_stat(e->ino, &e->attr));
}

static void fuse_lookup(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	size_t name_len = strlen(name) + 1;
	struct bpfs_inode *parent_inode;
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	parent_inode = get_inode(parent_ino);
	if (!parent_inode)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}

	dirent = find_dirent(parent_inode, name, name_len);
	if (!dirent)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}

	fill_fuse_entry(dirent, &e);
	xcall(fuse_reply_entry(req, &e));
}


#if 0
static void fuse_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
	Dprintf("%s(ino = %lu, nlookup = %lu)\n", __FUNCTION__, ino, nlookup);
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
	xcall(fuse_reply_attr(req, &stbuf, STDTIMEOUT));
}

static void fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
                         int to_set, struct fuse_file_info *fi)
{
	struct bpfs_inode *inode;
	struct bpfs_inode inode_tmp;
	struct stat stbuf;
	UNUSED(fi);

	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);

	// Why are bits 6, 7, and 8 set?
	// in fuse 2.8.1, 7 is atime_now and 8 is mtime_now. 6 is skipped.
	// assert(!(to_set & ~(FUSE_SET_ATTR_MODE | FUSE_SET_ATTR_UID | FUSE_SET_ATTR_GID | FUSE_SET_ATTR_SIZE | FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME)));

	inode = get_inode(ino);
	if (!inode)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	inode_tmp = *inode;

	if (to_set & FUSE_SET_ATTR_MODE)
		inode_tmp.mode = f2b_mode(attr->st_mode);
	if (to_set & FUSE_SET_ATTR_UID)
		inode_tmp.uid = attr->st_uid;
	if (to_set & FUSE_SET_ATTR_GID)
		inode_tmp.gid = attr->st_gid;
	if (to_set & FUSE_SET_ATTR_SIZE)
		inode_tmp.nbytes = attr->st_size;
	if (to_set & FUSE_SET_ATTR_ATIME)
		inode_tmp.atime.sec = attr->st_atime;
	if (to_set & FUSE_SET_ATTR_MTIME)
		inode_tmp.mtime.sec = attr->st_mtime;

	// TODO: make atomic. and optimize for subset of field changes?
	*inode = inode_tmp;

	bpfs_stat(ino, &stbuf);
	xcall(fuse_reply_attr(req, &stbuf, STDTIMEOUT));
}

#if 0
static void fuse_readlink(fuse_req_t req, fuse_ino_t ino)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino,);
}

static void fuse_mknod(fuse_req_t req, fuse_ino_t parent_ino, const char *name,
                       mode_t mode, dev_t rdev)
{
	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);
}
#endif

static void fuse_mkdir(fuse_req_t req, fuse_ino_t parent_ino, const char *name,
                       mode_t mode)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;
	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);
	int r = create_file(req, parent_ino, name, mode | S_IFDIR, &dirent);
	if (r < 0)
	{
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
	xcall(fuse_reply_entry(req, &e));
}

static void fuse_unlink(fuse_req_t req, fuse_ino_t parent_ino,
                        const char *name)
{
	struct bpfs_inode *parent_inode;
	struct bpfs_dirent *dirent;
	struct bpfs_inode *inode;
	uint64_t ino;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	parent_inode = get_inode(parent_ino);
	if (!parent_inode)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}

	dirent = find_dirent(parent_inode, name, strlen(name) + 1);
	if (!dirent)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}
	inode = get_inode(dirent->ino);
	assert(inode);
	ino = dirent->ino;

	dirent->ino = BPFS_INO_INVALID;

	if (inode->block_addr != BPFS_BLOCKNO_INVALID)
		free_block(inode->block_addr);
	free_inode(ino);

	xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
}

static void fuse_rmdir(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	struct bpfs_inode *parent_inode;
	struct bpfs_dirent *dirent;
	struct bpfs_inode *inode;
	uint64_t ino;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	parent_inode = get_inode(parent_ino);
	if (!parent_inode)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}

	dirent = find_dirent(parent_inode, name, strlen(name) + 1);
	if (!dirent)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}
	inode = get_inode(dirent->ino);
	assert(inode);
	ino = dirent->ino;

	if (inode->block_addr != BPFS_BLOCKNO_INVALID)
	{
		uint64_t off = 0;
		while (off < inode->nbytes)
		{
			struct bpfs_dirent *cdirent = (struct bpfs_dirent*) (get_block(inode->block_addr + off / BPFS_BLOCK_SIZE) + off % BPFS_BLOCK_SIZE);
			if (!cdirent->rec_len)
			{
				// end of directory entries
				break;
			}
			off += cdirent->rec_len;
			if (cdirent->ino == BPFS_INO_INVALID)
				continue;
			if (cdirent->ino == parent_ino)
				continue;
			xcall(fuse_reply_err(req, ENOTEMPTY));
			return;
		}
	}

	dirent->ino = BPFS_INO_INVALID;

	if (inode->block_addr != BPFS_BLOCKNO_INVALID)
		free_block(inode->block_addr);
	free_inode(ino);

	xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
}

#if 0
static void fuse_symlink(fuse_req_t req, const char *link,
                         fuse_ino_t parent_ino, const char *name)
{
	Dprintf("%s(link = '%s', parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, link, parent_ino, name);
}
#endif

static void fuse_rename(fuse_req_t req,
                        fuse_ino_t src_parent_ino, const char *src_name,
                        fuse_ino_t dst_parent_ino, const char *dst_name)
{
	struct bpfs_inode *src_parent;
	struct bpfs_dirent *src_dirent;
	struct bpfs_inode *dst_parent;
	struct bpfs_dirent *dst_dirent;
	size_t dst_name_len = strlen(dst_name) + 1;
	struct bpfs_inode *inode;
	struct bpfs_inode *unlinked_inode = NULL;
	uint64_t unlinked_ino = BPFS_INO_INVALID;

	Dprintf("%s(src_parent_ino = %lu, src_name = '%s',"
	        " dst_parent_ino = %lu, dst_name = '%s')\n",
	        __FUNCTION__, src_parent_ino, src_name, dst_parent_ino, dst_name);

	src_parent = get_inode(src_parent_ino);
	dst_parent = get_inode(dst_parent_ino);
	if (!src_parent || !dst_parent)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}

	src_dirent = find_dirent(src_parent, src_name, strlen(src_name) + 1);
	dst_dirent = find_dirent(dst_parent, dst_name, dst_name_len);
	if (!src_dirent)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}

	inode = get_inode(src_dirent->ino);
	assert(inode);

	if (dst_dirent)
	{
		// TODO: check that types match?
		unlinked_inode = get_inode(dst_dirent->ino);
		assert(unlinked_inode);
		unlinked_ino = dst_dirent->ino;
	}
	else
	{
		int r = alloc_dirent(dst_parent, dst_name, dst_name_len, &dst_dirent);
		if (r < 0)
		{
			xcall(fuse_reply_err(req, -r));
			return;
		}
		dst_dirent->file_type = src_dirent->file_type;
	}

	dst_dirent->ino = src_dirent->ino;
	src_dirent->ino = BPFS_INO_INVALID;

	if (unlinked_inode)
	{
		if (unlinked_inode->block_addr != BPFS_BLOCKNO_INVALID)
			free_block(unlinked_inode->block_addr);
		free_inode(unlinked_ino);
	}

	xcall(fuse_reply_err(req, FUSE_ERR_SUCCESS));
}

#if 0
static void fuse_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t parent_ino,
                      const char *name)
{
	Dprintf("%s(ino = %lu, parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, ino, parent_ino, name);
}


static void fuse_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
}
#endif

// FIXME: is readdir() supposed to not notice changes made after the opendir?
static void fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t max_size,
                         off_t off, struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	off_t total_size = 0;
	char *buf = NULL;

	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ")\n",
	        __FUNCTION__, ino, off);

	if (!inode)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	if (!(inode->mode & BPFS_S_IFDIR))
	{
		xcall(fuse_reply_err(req, ENOTDIR));
		return;
	}

	while (1)
	{
		off_t oldsize = total_size;
		const char *name;
		struct stat stbuf;
		size_t fuse_dirent_size;

		memset(&stbuf, 0, sizeof(stbuf));

		if (off == 0)
		{
			name = ".";
			stbuf.st_ino = ino;
			stbuf.st_mode |= S_IFDIR;
			off++;
		}
		else if (off + sizeof(struct bpfs_dirent) >= inode->nbytes + 1)
		{
			break;
		}
		else
		{
			struct bpfs_dirent *dirent = (struct bpfs_dirent*) (get_block(inode->block_addr) + off - 1);
			if (!dirent->rec_len)
			{
				// end of directory entries
				break;
			}
			off += dirent->rec_len;
			if (dirent->ino == BPFS_INO_INVALID)
				continue;
			name = dirent->name;
			stbuf.st_ino = dirent->ino;
			stbuf.st_mode = b2f_filetype(dirent->file_type);
		}

		fuse_dirent_size = fuse_add_direntry(req, NULL, 0, name, NULL, 0);
		if (total_size + fuse_dirent_size > max_size)
			break;
		total_size += fuse_dirent_size;
		buf = (char*) realloc(buf, total_size);
		if (!buf)
		{
			// PERHAPS: retry with a smaller max_size?
			xcall(fuse_reply_err(req, ENOMEM));
			free(buf);
			return;
		}
		fuse_add_direntry(req, buf + oldsize, max_size - oldsize,
		                  name, &stbuf, off);
	}
	xcall(fuse_reply_buf(req, buf, total_size));
	free(buf);
}

#if 0
static void fuse_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
}

static void fuse_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                          struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu, datasync = %d)\n", __FUNCTION__, ino, datasync);
}
#endif


static void fuse_create(fuse_req_t req, fuse_ino_t parent_ino,
                        const char *name, mode_t mode,
                        struct fuse_file_info *fi)
{
	struct bpfs_dirent *dirent;
	struct fuse_entry_param e;

	Dprintf("%s(parent_ino = %lu, name = '%s')\n",
	        __FUNCTION__, parent_ino, name);

	int r = create_file(req, parent_ino, name, mode, &dirent);
	if (r < 0)
	{
		xcall(fuse_reply_err(req, -r));
		return;
	}

	fill_fuse_entry(dirent, &e);
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
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}
	if (inode->mode & BPFS_S_IFDIR)
	{
		xcall(fuse_reply_err(req, EISDIR));
		return;
	}
	// TODO: should we detect EACCES?

	// TODO: fi->flags: O_APPEND, O_NOATIME?

	xcall(fuse_reply_open(req, fi));
}

static void fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	char *data;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ", size = %zu)\n",
	        __FUNCTION__, ino, off, size);

	if (!inode)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}
	assert(inode->mode & BPFS_S_IFREG);
	assert(inode->nbytes <= inode->nblocks * BPFS_BLOCK_SIZE);
	if (off >= inode->nbytes)
	{
		xcall(fuse_reply_buf(req, NULL, 0));
		return;
	}
	assert(off + size <= BPFS_BLOCK_SIZE);
	data = get_block(inode->block_addr);
	xcall(fuse_reply_buf(req, data + off, MIN(size, inode->nbytes - off)));
}

static void fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	char *data;
	UNUSED(fi);

	Dprintf("%s(ino = %lu, off = %" PRId64 ", size = %zu)\n",
	        __FUNCTION__, ino, off, size);

	if (!inode)
	{
		xcall(fuse_reply_err(req, ENOENT));
		return;
	}
	assert(inode->mode & BPFS_S_IFREG);

	// FIXME: for now
	assert(off + size <= BPFS_BLOCK_SIZE);
	if (off / BPFS_BLOCK_SIZE != (off + size) / BPFS_BLOCK_SIZE)
	{
		xcall(fuse_reply_err(req, EINVAL));
		return;
	}

	if (inode->nblocks * BPFS_BLOCK_SIZE < off + size)
	{
		// FIXME: assumes single block file
		inode->block_addr = alloc_block();
		if (inode->block_addr == BPFS_BLOCKNO_INVALID)
		{
			xcall(fuse_reply_err(req, ENOSPC));
			return;
		}
		inode->nblocks++;
	}

	data = get_block(inode->block_addr);
	memcpy(data + off, buf, size);
	if (off + size > inode->nbytes)
		inode->nbytes = off + size;
	xcall(fuse_reply_write(req, size));
}

#if 0
static void fuse_flush(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
	xcall(fuse_reply_err(req, ENOSYS));
}

static void fuse_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu)\n", __FUNCTION__, ino);
}

static void fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                       struct fuse_file_info *fi)
{
	Dprintf("%s(ino = %lu, datasync = %d)\n", __FUNCTION__, ino, datasync);
}
#endif


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
//	ADD_FUSE_CALLBACK(readlink);
//	ADD_FUSE_CALLBACK(mknod);
	ADD_FUSE_CALLBACK(mkdir);
	ADD_FUSE_CALLBACK(unlink);
	ADD_FUSE_CALLBACK(rmdir);
//	ADD_FUSE_CALLBACK(symlink);
	ADD_FUSE_CALLBACK(rename);
//	ADD_FUSE_CALLBACK(link);

//	ADD_FUSE_CALLBACK(setxattr);
//	ADD_FUSE_CALLBACK(getxattr);
//	ADD_FUSE_CALLBACK(listxattr);
//	ADD_FUSE_CALLBACK(removexattr);

//	ADD_FUSE_CALLBACK(opendir);
	ADD_FUSE_CALLBACK(readdir);
//	ADD_FUSE_CALLBACK(releasedir);
//	ADD_FUSE_CALLBACK(fsyncdir);

	ADD_FUSE_CALLBACK(create);
	ADD_FUSE_CALLBACK(open);
	ADD_FUSE_CALLBACK(read);
	ADD_FUSE_CALLBACK(write);
//	ADD_FUSE_CALLBACK(flush);
//	ADD_FUSE_CALLBACK(release);
//	ADD_FUSE_CALLBACK(fsync);

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
	assert(!bpram && !bpram_size);
	bpram = malloc(size);
	xassert(bpram);
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

	super = (struct bpfs_super*) bpram;

	if (super->magic != BPFS_FS_MAGIC)
	{
		fprintf(stderr, "Not a BPFS file system (incorrect magic)\n");
		return -1;
	}
	if (super->nblocks * BPFS_BLOCK_SIZE < bpram_size)
	{
		fprintf(stderr, "BPRAM is smaller than the file system\n");
		return -1;
	}

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
