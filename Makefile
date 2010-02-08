CC = gcc
CFLAGS = -Wall -g
# Remove debug checks:
#CFLAGS += -DNDEBUG
# Add optimizations:
#CFLAGS += -O3

.PHONY: all clean

BIN = bpfs mkfs.bpfs
OBJS = bpfs.o mkfs.bpfs.o mkbpfs.o
TAGS = tags TAGS
SRCS = bpfs.c mkfs.bpfs.c mkbpfs.c mkbpfs.h bpfs_structs.h util.h

all: $(BIN) $(TAGS)

clean:
	rm -f $(BIN) $(OBJS) $(TAGS)

tags: $(SRCS)
	if ctags --version | grep -q Exuberant; then ctags $(SRCS); else touch $@; fi
TAGS: $(SRCS)
	if ctags --version | grep -q Exuberant; then ctags -e $(SRCS); else touch $@; fi

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
