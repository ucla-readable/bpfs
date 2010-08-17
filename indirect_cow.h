/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#ifndef INDIRECT_COW_H
#define INDIRECT_COW_H

#include <stdint.h>

int indirect_cow_init(void);
void indirect_cow_destroy(void);

// Push when a block pointer is followed. Pop on the return.
int indirect_cow_parent_push(uint64_t blkno);
void indirect_cow_parent_pop(uint64_t blkno);

int indirect_cow_block_cow(uint64_t orig_blkno, uint64_t cow_blkno);
char* indirect_cow_block_get(uint64_t blkno);
void indirect_cow_block_required(uint64_t blkno);
// Write the changes in this region immediately if blkno is CoWed
void indirect_cow_block_direct(uint64_t blkno, unsigned off, unsigned size);

void indirect_cow_commit(void);
void indirect_cow_abort(void);

uint64_t get_super_blockno(void);

#endif
