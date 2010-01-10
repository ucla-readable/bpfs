CC = gcc
CFLAGS = -Wall -g

.PHONY: all clean

BIN = bpfs mkfs.bpfs
OBJS = bpfs.o mkfs.bpfs.o mkbpfs.o
TAGS = tags TAGS

all: $(BIN) $(TAGS)

clean:
	rm -f $(BIN) $(OBJS) $(TAGS)

tags: bpfs.c mkfs.bpfs.c mkbpfs.c mkbpfs.h bpfs_structs.h util.h
	if ctags --version | grep -q Exuberant; then ctags -R; else touch $@; fi
TAGS: bpfs.c mkfs.bpfs.c mkbpfs.c mkbpfs.h bpfs_structs.h util.h
	if ctags --version | grep -q Exuberant; then ctags -R -e; else touch $@; fi

bpfs.o: bpfs.c mkbpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) `pkg-config --cflags fuse` -c -o $@ $<

mkfs.bpfs.o: mkfs.bpfs.c mkbpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

mkbpfs.o: mkbpfs.c mkbpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

bpfs: bpfs.o mkbpfs.o
	$(CC) $(CFLAGS) `pkg-config --libs fuse` -luuid -o $@ $^

mkfs.bpfs: mkfs.bpfs.o mkbpfs.o
	$(CC) $(CFLAGS) -luuid -o $@ $^
