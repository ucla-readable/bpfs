/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "dcache.h"
#include "util.h"
#include "hash_map.h"

#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

// NOTE: It may be faster to use pools for these data-structures.


// Fixed-size cache for now. Must be at least 2, for rename. 1024? Why not.
#define NMDIRS_MAX 1024

struct mdirent_free
{
	uint64_t off;
	struct mdirent_free *next;
	uint16_t rec_len;
};

struct mdirectory
{
	hash_map_t *dirents; // name -> mdirent
	struct mdirent_free *free_dirents;
	uint64_t ino; // inode number of this directory
	struct mdirectory **lru_polder, *lru_newer;
};

struct dcache {
	hash_map_t *directories; // directory ino -> mdirectory
	struct mdirectory *lru_oldest, *lru_newest;
};


static struct dcache dcache;


// mdirent

static void mdirent_free(struct mdirent *md)
{
	free((char*) md->name);
	free(md);
}

static struct mdirent* mdirent_dup(const struct mdirent *md)
{
	struct mdirent *dup = malloc(sizeof(*dup));
	if (!dup)
		return NULL;
	memcpy(dup, md, sizeof(*dup));

	dup->name = strdup(md->name);
	if (!dup->name)
	{
		free(dup);
		return NULL;
	}

	return dup;
}


// mdirectory

static void mdirectory_touch(struct mdirectory *mdir)
{
	assert(dcache.lru_newest && dcache.lru_oldest);

	if (!mdir->lru_newer)
		return; // mdir is already the head

	// Remove mdir from the LRU
	mdir->lru_newer->lru_polder = mdir->lru_polder;
	*mdir->lru_polder = mdir->lru_newer;

	// Add mdir to the head of the LRU
	mdir->lru_polder = &dcache.lru_newest->lru_newer;
	*mdir->lru_polder = mdir;
	mdir->lru_newer = NULL;
	dcache.lru_newest = mdir;
}

static void mdirectory_rem(struct mdirectory *mdir)
{
	hash_map_it2_t it = hash_map_it2_create(mdir->dirents);

	while (hash_map_it2_next(&it))
		mdirent_free(it.val);
	hash_map_destroy(mdir->dirents);

	hash_map_erase(dcache.directories, u64_ptr(mdir->ino));

	// Remove mdir from the LRU
	if (mdir->lru_newer)
		mdir->lru_newer->lru_polder = mdir->lru_polder;
	else if (mdir->lru_polder != &dcache.lru_oldest)
		dcache.lru_newest = container_of(mdir->lru_polder, struct mdirectory,
		                                 lru_newer);
	else
		dcache.lru_newest = NULL;
	*mdir->lru_polder = mdir->lru_newer;

	while (mdir->free_dirents)
	{
		struct mdirent_free *next = mdir->free_dirents->next;
		free(mdir->free_dirents);
		mdir->free_dirents = next;
	}

	free(mdir);
}

static struct mdirectory* mdirectory_add(uint64_t ino)
{
	struct mdirectory *mdir;
	int r;

	if (hash_map_size(dcache.directories) == NMDIRS_MAX)
		mdirectory_rem(dcache.lru_oldest);

	mdir = malloc(sizeof(*mdir));
	if (!mdir)
		return NULL;

	mdir->dirents = hash_map_create_str();
	if (!mdir->dirents)
		goto oom_mdir;

	mdir->free_dirents = NULL;

	mdir->ino = ino;

	r = hash_map_insert(dcache.directories, u64_ptr(ino), mdir);
	if (r < 0)
		goto oom_dirents;

	// Add mdir to the head of the LRU
	if (dcache.lru_newest)
		mdir->lru_polder = &dcache.lru_newest->lru_newer;
	else
		mdir->lru_polder = &dcache.lru_oldest;
	*mdir->lru_polder = mdir;
	mdir->lru_newer = NULL;
	dcache.lru_newest = mdir;

	return mdir;

  oom_dirents:
	hash_map_destroy(mdir->dirents);
  oom_mdir:
	free(mdir);
	return NULL;
}


// external API

int dcache_init(void)
{
	assert(!dcache.directories);

	dcache.directories = hash_map_create_size_ptr(NMDIRS_MAX, 0);
	if (!dcache.directories)
		return -ENOMEM;

	dcache.lru_newest = dcache.lru_oldest = NULL;

	return 0;
}

void dcache_destroy(void)
{
	hash_map_it2_t it = hash_map_it2_create(dcache.directories);
	while (hash_map_it2_next(&it))
		mdirectory_rem(it.val);

	hash_map_destroy(dcache.directories);
	dcache.directories = NULL;
	dcache.lru_newest = dcache.lru_oldest = NULL;
}


bool dcache_has_dir(uint64_t ino)
{
	return !!hash_map_find_val(dcache.directories, u64_ptr(ino));
}

int dcache_add_dir(uint64_t ino)
{
	struct mdirectory *mdir;
	assert(!hash_map_find_val(dcache.directories, u64_ptr(ino)));
	mdir = mdirectory_add(ino);
	if (!mdir)
		return -ENOMEM;
	return 0;
}

void dcache_rem_dir(uint64_t ino)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(ino));
	assert(mdir);
	mdirectory_rem(mdir);
}

int dcache_add_dirent(uint64_t parent_ino, const char *name,
                      const struct mdirent *mdo)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(parent_ino));
	struct mdirent *mdc;
	int r;

	assert(mdir);
	mdirectory_touch(mdir);

	mdc = mdirent_dup(mdo);
	if (!mdc)
		return -ENOMEM;

	r = hash_map_insert(mdir->dirents, (void*) mdc->name, mdc);
	if (r < 0)
	{
		mdirent_free(mdc);
		return r;
	}
	assert(!r);

	return 0;
}

const struct mdirent* dcache_get_dirent(uint64_t parent_ino, const char *name)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(parent_ino));
	assert(mdir);
	mdirectory_touch(mdir);
	return hash_map_find_val(mdir->dirents, name);
}

int dcache_rem_dirent(uint64_t parent_ino, const char *name)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(parent_ino));
	struct mdirent *md;

	assert(mdir);
	mdirectory_touch(mdir);

	md = hash_map_erase(mdir->dirents, name);
	if (!md)
		return -EINVAL;

	mdirent_free(md);

	return 0;
}


int dcache_add_free(uint64_t parent_ino, uint64_t off, uint16_t rec_len)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(parent_ino));
	struct mdirent_free *mdf;

	assert(mdir);
	assert(off != DCACHE_FREE_NONE);

	mdf = malloc(sizeof(*mdf));
	if (!mdf)
		return -ENOMEM;
	mdf->off = off;
	mdf->rec_len = rec_len;
	mdf->next = mdir->free_dirents;
	mdir->free_dirents = mdf;

	return 0;
}

uint64_t dcache_take_free(uint64_t parent_ino, uint16_t min_rec_len)
{
	struct mdirectory *mdir = hash_map_find_val(dcache.directories,
	                                            u64_ptr(parent_ino));
	struct mdirent_free *prev_mdf, *mdf;

	assert(mdir);

	for (prev_mdf = NULL, mdf = mdir->free_dirents; mdf;
	     prev_mdf = mdf, mdf = mdf->next)
	{
		if (mdf->rec_len >= min_rec_len)
		{
			uint64_t off = mdf->off;
			if (prev_mdf)
				prev_mdf->next = mdf->next;
			else
				mdir->free_dirents = mdf->next;
			free(mdf);
			return off;
		}
	}

	return DCACHE_FREE_NONE;
}
