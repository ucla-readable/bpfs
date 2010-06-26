CC = gcc
CFLAGS = -Wall -g
# Remove debug checks:
#CFLAGS += -DNDEBUG
# Add optimizations:
#CFLAGS += -O3

.PHONY: all clean

BIN = bpfs mkfs.bpfs pwrite
OBJS = bpfs.o mkfs.bpfs.o mkbpfs.o hash_map.o vector.o
TAGS = tags TAGS
SRCS = bpfs.c mkfs.bpfs.c mkbpfs.c mkbpfs.h bpfs_structs.h util.h hash_map.c hash_map.h vector.c vector.h pool.h pwrite.c

all: $(BIN) $(TAGS)

clean:
	rm -f $(BIN) $(OBJS) $(TAGS)

tags: $(SRCS)
	@echo + ctags tags
	@if ctags --version | grep -q Exuberant; then ctags $(SRCS); else touch $@; fi
TAGS: $(SRCS)
	@echo + ctags TAGS
	@if ctags --version | grep -q Exuberant; then ctags -e $(SRCS); else touch $@; fi

bpfs.o: bpfs.c mkbpfs.h bpfs_structs.h util.h hash_map.h
	$(CC) $(CFLAGS) `pkg-config --cflags fuse` -c -o $@ $<

mkfs.bpfs.o: mkfs.bpfs.c mkbpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

mkbpfs.o: mkbpfs.c mkbpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

vector.o: vector.c vector.h
	$(CC) $(CFLAGS) -c -o $@ $<

hash_map.o: hash_map.c hash_map.h vector.h pool.h
	$(CC) $(CFLAGS) -c -o $@ $<

bpfs: bpfs.o mkbpfs.o hash_map.o vector.o
	$(CC) $(CFLAGS) `pkg-config --libs fuse` -luuid -o $@ $^

mkfs.bpfs: mkfs.bpfs.o mkbpfs.o
	$(CC) $(CFLAGS) -luuid -o $@ $^
