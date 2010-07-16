/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2
 * of the GNU GPL. See the file LICENSE for details. */

#define _XOPEN_SOURCE 500

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

int main(int argc, char **argv)
{
	char *filename = argv[1], *str = argv[2], *offset = argv[3];
	int fd, r;

	if (argc != 4)
	{
		fprintf(stderr, "Overwrite a range of bytes in a file.\n");
		fprintf(stderr, "Usage: %s <FILE> <STRING> <OFFSET>\n", argv[0]);
		return 1;
	}

	fd = open(filename, O_RDWR);
	assert(fd >= 0);

	r = pwrite(fd, str, strlen(str), atoi(offset));
	assert(r == strlen(str));

	r = close(fd);
	assert(r >= 0);

	return 0;
}
