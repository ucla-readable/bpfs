/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef CRAWLER_H
#define CRAWLER_H

#include "bpfs.h"
#include "bpfs_structs.h"

#include <stdbool.h>
#include <stdint.h>


void crawler_init(void);


// @param blockoff block no in the file (blockoff * BPFS_BLOCK_SIZE is byte off)
// @param block pointer to the block
// @param off offset into the block
// @param valid number of valid bytes in the block
// @param crawl_start byte offset into the file at which the crawl started
// @param commit allowed commit type
// @param user user data
// @param blockno *blockno is the block number (in/out)
// Return <0 for error, 0 for success, 1 for success and stop crawl
typedef int (*crawl_callback)(uint64_t blockoff, char *block,
                              unsigned off, unsigned size, unsigned valid,
                              uint64_t crawl_start, enum commit commit,
                              void *user, uint64_t *blockno);

typedef void (*crawl_blockno_callback)(uint64_t blockno, bool leaf);

// Return <0 for error, 0 for success, 1 for success and stop crawl
typedef int (*crawl_callback_inode)(char *block, unsigned off,
                                    struct bpfs_inode *inode,
                                    enum commit commit, void *user,
                                    uint64_t *blockno);


int crawl_tree(struct bpfs_tree_root *root, uint64_t off,
               uint64_t size, enum commit commit,
               crawl_callback callback, void *user,
               uint64_t *prev_blockno);

void crawl_blocknos(const struct bpfs_tree_root *root,
                    uint64_t off, uint64_t size,
                    crawl_blockno_callback callback);

int crawl_inodes(uint64_t off, uint64_t size, enum commit commit,
                 crawl_callback callback, void *user);

int crawl_inode(uint64_t ino, enum commit commit,
                crawl_callback_inode callback, void *user);

int crawl_data(uint64_t ino, uint64_t off, uint64_t size,
               enum commit commit,
               crawl_callback callback, void *user);

int crawl_data_2(uint64_t ino_0, uint64_t off_0, uint64_t size_0,
                 crawl_callback callback_0, void *user_0,
                 uint64_t ino_1, uint64_t off_1, uint64_t size_1,
                 crawl_callback callback_1, void *user_1,
                 enum commit commit);

#endif
