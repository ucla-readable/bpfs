/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef DCACHE_H
#define DCACHE_H

#include <inttypes.h>
#include <stdbool.h>

struct mdirent
{
	uint64_t off;
	uint64_t ino;
	uint64_t ino_generation;
	uint8_t file_type;
};

static __inline
void mdirent_init(struct mdirent *md,
                  uint64_t off, uint64_t ino, uint64_t ino_gen, uint8_t ft)
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
// Inline implementation

static __inline
void mdirent_init(struct mdirent *md,
                  uint64_t off, uint64_t ino, uint64_t ino_gen, uint8_t ft)
{
	md->off = off;
	md->ino = ino;
	md->ino_generation = ino_gen;
	md->file_type = ft;
}

#endif
