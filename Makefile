.PHONY: all clean

all: bpfs mkfs.bpfs tags TAGS

clean:
	rm -f bpfs mkfs.bpfs tags TAGS

tags: bpfs.c mkfs.bpfs.c bpfs_structs.h util.h
	if ctags --version | grep -q Exuberant; then ctags -R; else touch $@; fi
TAGS: bpfs.c mkfs.bpfs.c bpfs_structs.h util.h
	if ctags --version | grep -q Exuberant; then ctags -R -e; else touch $@; fi

bpfs: bpfs.c bpfs_structs.h util.h
	gcc -g -Wall `pkg-config --cflags --libs fuse` -o $@ $<

mkfs.bpfs: mkfs.bpfs.c bpfs_structs.h util.h
	gcc -g -Wall -luuid -o $@ $<
