#include "bpfs_structs.h"
#include "util.h"

#define FUSE_USE_VERSION 26
#include <fuse/fuse_lowlevel.h>

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// TODO:
// - make time higher resolution. See ext4, bits/stat.h, linux/time.h.
// - add gcc flag to warn about ignored return values?


// define to 1 if output files are created at exit (eg gprof or massif)
#ifndef RETURN_WD
# define RETURN_WD 0
#endif

// STDTIMEOUT is not 0 because of a fuse kernel module bug.
// Miklos's 2006/06/27 email, E1FvBX0-0006PB-00@dorka.pomaz.szeredi.hu, fixes.
#define STDTIMEOUT 1.0


static char *bpram;
static size_t bpram_size;

static struct bpfs_super *super;


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
			xtest(1);
	}
	return mode;
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
		default: xtest(1);
	}
	fmode |= bmode & BPFS_S_IPERM; // TODO: correct?
	return fmode;
}

static char* get_block(uint64_t no)
{
	assert(no != BPFS_BLOCKNO_INVALID);
	assert(no <= super->nblocks);
	return bpram + (no - 1) * BPFS_BLOCK_SIZE;
}

static struct bpfs_inode * get_inode(uint64_t ino)
{
	struct bpfs_inode *inodes = (struct bpfs_inode*) get_block(super->inode_addr);
	if (ino == BPFS_INO_INVALID)
		return NULL;
	ino -= BPFS_INO_ROOT;
	if (ino >= super->ninodeblocks * BPFS_BLOCK_SIZE / sizeof(*inodes))
		return NULL;
	return &inodes[ino];
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
	stbuf->st_size = inode->nbytes;
	stbuf->st_blocks = inode->nblocks * BPFS_BLOCK_SIZE / 512;
	stbuf->st_atime = inode->atime.sec;
	stbuf->st_mtime = inode->mtime.sec;
	stbuf->st_ctime = inode->ctime.sec;
	return 0;
}


static void bpfs_init(void *userdata, struct fuse_conn_info *conn)
{
}

static void bpfs_destroy(void *userdata)
{
}


static void bpfs_statfs(fuse_req_t req, fuse_ino_t ino)
{
}


static void bpfs_lookup(fuse_req_t req, fuse_ino_t parent_ino, const char *name)
{
	size_t name_len = strlen(name) + 1;
	struct bpfs_inode *parent_inode;
	uint64_t off = 0;
	struct bpfs_dirent *dirent;

	parent_inode = get_inode(parent_ino);
	if (!parent_inode)
	{
		fuse_reply_err(req, ENOENT);
		return;
	}

	assert(parent_inode->block_addr != BPFS_BLOCKNO_INVALID);

	while (off < parent_inode->nbytes)
	{
		dirent = (struct bpfs_dirent*) (get_block(parent_inode->block_addr + off / BPFS_BLOCK_SIZE) + off % BPFS_BLOCK_SIZE);
		if (!dirent->rec_len)
		{
			// end of directory entries
			break;
		}
		off += dirent->rec_len;
		if (!dirent->ino)
			continue;
		if (name_len == dirent->name_len && !strcmp(name, dirent->name))
		{
			struct fuse_entry_param e;
			memset(&e, 0, sizeof(e));
			e.ino = dirent->ino;
			e.attr_timeout = STDTIMEOUT;
			e.entry_timeout = STDTIMEOUT;
			xtest(bpfs_stat(e.ino, &e.attr));
			fuse_reply_entry(req, &e);
			return;
		}
	}

	fuse_reply_err(req, ENOENT);
}

static void bpfs_forget(fuse_req_t req, fuse_ino_t ino, unsigned long nlookup)
{
}

// Not implemented: bpfs_access() (use default_permissions instead)

static void bpfs_getattr(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
	struct stat stbuf;
	UNUSED(fi);

	bpfs_stat(ino, &stbuf);
	fuse_reply_attr(req, &stbuf, STDTIMEOUT);
}

static void bpfs_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			             int to_set, struct fuse_file_info *fi)
{
}

static void bpfs_readlink(fuse_req_t req, fuse_ino_t ino)
{
}

static void bpfs_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode, dev_t rdev)
{
}

static void bpfs_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode)
{
}

static void bpfs_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
}

static void bpfs_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
}

static void bpfs_symlink(fuse_req_t req, const char *link, fuse_ino_t parent,
                         const char *name)
{
}

static void bpfs_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
                        fuse_ino_t newparent, const char *newname)
{
}

static void bpfs_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
                      const char *newname)
{
}


// TODO: if XATTR_SUPPRT (recall that OSX adds a position param):
// bpfs_setxattr, bpfs_getxattr, bpfs_listxattr, bpfs_removexattr

static void bpfs_opendir(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
}

// FIXME: is readdir() supposed to not notice changes made after the opendir?
static void bpfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t max_size,
                         off_t off, struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	off_t total_size = 0;
	char *buf = NULL;
	int err;

	UNUSED(fi);

	if (!inode)
	{
		fuse_reply_err(req, EINVAL);
		return;
	}
	if (!(inode->mode & BPFS_S_IFDIR))
	{
		fuse_reply_err(req, ENOTDIR);
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
		else if (off + BPFS_DIRENT_LEN >= inode->nbytes + 1)
		{
			break;
		}
		else
		{
			struct bpfs_inode *inode = (struct bpfs_inode*) get_block(super->inode_addr);
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
		buf = (char *) realloc(buf, total_size);
		if (!buf)
		{
			// PERHAPS: retry with a smaller max_size?
			err = fuse_reply_err(req, ENOMEM); 
			xtest(err);
			free(buf);
			return;
		}
		fuse_add_direntry(req, buf + oldsize, max_size - oldsize,
		                  name, &stbuf, off);
	}
	err = fuse_reply_buf(req, buf, total_size);
	xtest(err);
	free(buf);
}

static void bpfs_releasedir(fuse_req_t req, fuse_ino_t ino,
                            struct fuse_file_info *fi)
{
}

static void bpfs_fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync,
                          struct fuse_file_info *fi)
{
}


static void bpfs_create(fuse_req_t req, fuse_ino_t parent, const char *name,
                       mode_t mode, struct fuse_file_info *fi)
{
}

static void bpfs_open(fuse_req_t req, fuse_ino_t ino,
                      struct fuse_file_info *fi)
{
	if (ino != 2)
		fuse_reply_err(req, EISDIR);
	else if ((fi->flags & 3) != O_RDONLY)
		fuse_reply_err(req, EACCES);
	else
		fuse_reply_open(req, fi);
}

static int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
			     off_t off, size_t maxsize)
{
	if (off < bufsize)
		return fuse_reply_buf(req, buf + off,
				      MIN(bufsize - off, maxsize));
	else
		return fuse_reply_buf(req, NULL, 0);
}

static void bpfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
                      struct fuse_file_info *fi)
{
	struct bpfs_inode *inode = get_inode(ino);
	char *data;
	UNUSED(fi);
	if (!inode)
	{
		fuse_reply_err(req, ENOENT);
		return;
	}
	assert(inode->mode & BPFS_S_IFREG);
	if (off >= inode->nbytes)
	{
		fuse_reply_buf(req, NULL, 0);
		return;
	}
	assert(off + size <= BPFS_BLOCK_SIZE);
	data = get_block(inode->block_addr);
	xtest(fuse_reply_buf(req, data + off, MIN(size, inode->nbytes - off)));
}

static void bpfs_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
                       size_t size, off_t off, struct fuse_file_info *fi)
{
}

static void bpfs_flush(fuse_req_t req, fuse_ino_t ino,
                       struct fuse_file_info *fi)
{
	fprintf(stderr, "%s\n", __FUNCTION__);
	fuse_reply_err(req, ENOSYS);
}

static void bpfs_release(fuse_req_t req, fuse_ino_t ino,
                         struct fuse_file_info *fi)
{
}

static void bpfs_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
                       struct fuse_file_info *fi)
{
}


// TODO: bpfs_getlk, bpfs_setlk

// Doesn't make sense to implement: bpfs_bmap (no backing block device)


static void init_bpfs_ops(struct fuse_lowlevel_ops *fuse_ops)
{
	memset(fuse_ops, 0, sizeof(*fuse_ops));

#define ADD_FUSE_CALLBACK(name) fuse_ops->name = bpfs_##name

	ADD_FUSE_CALLBACK(init);
	ADD_FUSE_CALLBACK(destroy);

/*
	ADD_FUSE_CALLBACK(statfs);
*/
	ADD_FUSE_CALLBACK(lookup);
/*
	ADD_FUSE_CALLBACK(forget);
*/
	ADD_FUSE_CALLBACK(getattr);
/*
	ADD_FUSE_CALLBACK(setattr);
	ADD_FUSE_CALLBACK(readlink);
	ADD_FUSE_CALLBACK(mknod);
	ADD_FUSE_CALLBACK(mkdir);
	ADD_FUSE_CALLBACK(unlink);
	ADD_FUSE_CALLBACK(rmdir);
	ADD_FUSE_CALLBACK(symlink);
	ADD_FUSE_CALLBACK(rename);
	ADD_FUSE_CALLBACK(link);

	ADD_FUSE_CALLBACK(opendir);
*/
	ADD_FUSE_CALLBACK(readdir);
/*
	ADD_FUSE_CALLBACK(releasedir);
	ADD_FUSE_CALLBACK(fsyncdir);

	ADD_FUSE_CALLBACK(create);
*/
	ADD_FUSE_CALLBACK(open);
	ADD_FUSE_CALLBACK(read);
/*
	ADD_FUSE_CALLBACK(write);
	ADD_FUSE_CALLBACK(flush);
	ADD_FUSE_CALLBACK(release);
	ADD_FUSE_CALLBACK(fsync);
*/

#undef ADD_FUSE_CALLBACK
}

int main(int argc, char **argv)
{
	struct fuse_args fargs = FUSE_ARGS_INIT(argc, argv);
	struct fuse_lowlevel_ops fuse_ops;
	struct fuse_session *se;
	struct fuse_chan *ch;
	char *mountpoint;
	struct stat stbuf;
	int bpram_fd;
	int r = -1;
#if RETURN_WD
	char cwd[PATH_MAX];
#endif

	static_assert(FUSE_ROOT_ID == BPFS_INO_ROOT);

	bpram_fd = xsyscall(open("bpram", O_RDWR));
	
	xsyscall(fstat(bpram_fd, &stbuf));
	bpram_size = stbuf.st_size;
	xtest(bpram_size != stbuf.st_size);

	bpram = mmap(NULL, bpram_size, PROT_READ | PROT_WRITE, MAP_SHARED, bpram_fd, 0);
	xtest(!bpram);

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

	init_bpfs_ops(&fuse_ops);

#if RETURN_WD
	xtest(!getcwd(cwd, sizeof(cwd)));
#endif

	xtest(fuse_parse_cmdline(&fargs, &mountpoint, NULL, NULL) == -1);
	xtest((ch = fuse_mount(mountpoint, &fargs)) == NULL);

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
	fuse_opt_free_args(&fargs);

#if RETURN_WD
	xsyscall(chdir(cwd));
#endif

	xsyscall(msync(bpram, bpram_size, MS_SYNC));
	xsyscall(munmap(bpram, bpram_size));
	xsyscall(close(bpram_fd));

	return r;
}
