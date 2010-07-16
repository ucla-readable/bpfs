/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#include "mkbpfs.h"
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

int main(int argc, char **argv)
{
	char *bpram_name;
	int bpram_fd;
	struct stat stbuf;
	char *bpram;
	size_t bpram_size;

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

	bpram = mmap(NULL, bpram_size, PROT_READ | PROT_WRITE, MAP_SHARED, bpram_fd, 0);
	xassert(bpram);

	xcall(mkbpfs(bpram, bpram_size));

	xsyscall(msync(bpram, bpram_size, MS_SYNC));
	xsyscall(munmap(bpram, bpram_size));
	xsyscall(close(bpram_fd));

	return 0;
}
