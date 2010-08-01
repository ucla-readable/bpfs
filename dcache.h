/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef DCACHE_H
#define DCACHE_H

#include <inttypes.h>
#include <stdbool.h>

struct mdirent
{
	const char *name;
	uint64_t off;
	uint64_t ino;
	uint64_t ino_generation;
	uint16_t rec_len;
	uint8_t file_type;
};

static __inline
void mdirent_init(struct mdirent *md,
                  const char *name, uint64_t off, uint64_t ino,
                  uint64_t ino_gen, uint16_t rec_len, uint8_t ft)
	__attribute__((always_inline));

//
// The directory entry cache

int dcache_init(void);
void dcache_destroy(void);

//
// Directories

// Return whether the directory ino is currently in the dcache.
bool dcache_has_dir(uint64_t ino);

// Add the ino directory.
int dcache_add_dir(uint64_t ino);

// Remove the ino directory and its contents.
void dcache_rem_dir(uint64_t ino);

//
// Directory entries

// Add <name, md> to the parent_ino directory.
int dcache_add_dirent(uint64_t parent_ino, const char *name,
                      const struct mdirent *md);

// Get the dirent for <parent_ino, name>.
// parent_ino must be in the dcache.
const struct mdirent* dcache_get_dirent(uint64_t parent_ino, const char *name);

// Remove the dirent for name from the parent_ino directory.
// parent_ino must be in the dcache.
int dcache_rem_dirent(uint64_t parent_ino, const char *name);

//
// Free directory entries

// Add a free dirent.
int dcache_add_free(uint64_t parent_ino, uint64_t off, uint16_t rec_len);

#define DCACHE_FREE_NONE UINT64_MAX

// Find a free dirent with a rec_len at least min_rec_len,
// remove it from the set of free dirents, and return its offset.
// Returns DCACHE_FREE_NONE if none is found.
uint64_t dcache_take_free(uint64_t parent_ino, uint16_t min_rec_len);

//
// Inline implementation

static __inline
void mdirent_init(struct mdirent *md,
                  const char *name, uint64_t off, uint64_t ino,
                  uint64_t ino_gen, uint16_t rec_len, uint8_t ft)
{
	md->name = name;
	md->off = off;
	md->ino = ino;
	md->ino_generation = ino_gen;
	md->rec_len = rec_len;
	md->file_type = ft;
}

#endif
