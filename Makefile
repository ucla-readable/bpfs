LD = ld
CC = gcc
CFLAGS = -Wall 
# Remove debug checks:
CFLAGS += -DNDEBUG
# Enable optimizations:
CFLAGS += -O3 -march=native # GCC >4.4: -flto
# Enable Nehalem optimizations (GCC 4.4 -march only knows up to Core2):
#CFLAGS += -msahf -msse4 -msse4.1 -msse4.2
# Enable gprof:
#CFLAGS += -pg
# Enable debugging symbols
CFLAGS += -g

.PHONY: all clean

BIN = bpfs mkfs.bpfs pwrite
OBJS = bpfs.o crawler.o indirect_cow.o mkfs.bpfs.o mkbpfs.o dcache.o \
       hash_map.o vector.o
TAGS = tags TAGS
SRCS = bpfs_structs.h bpfs.h bpfs.c crawler.h crawler.c dcache.h dcache.c \
       indirect_cow.h indirect_cow.c mkbpfs.h mkbpfs.c mkfs.bpfs.c \
       util.h hash_map.h hash_map.c vector.h vector.c pool.h pwrite.c
# Non-compile sources (at least, for this Makefile):
NCSRCS = bench/bpramcount.cpp bench/microbench.py

all: $(BIN) $(TAGS)

love :
	@echo "Not war"
clean:
	rm -f $(BIN) $(OBJS) $(TAGS)

tags: $(SRCS) $(NCSRCS)
	@echo + ctags tags
	@if ctags --version | grep -q Exuberant; then ctags $(SRCS) $(NCSRCS); else touch $@; fi
TAGS: $(SRCS) $(NCSRCS)
	@echo + ctags TAGS
	@if ctags --version | grep -q Exuberant; then ctags -e $(SRCS) $(NCSRCS); else touch $@; fi

bpfs.o: bpfs.c bpfs_structs.h bpfs.h crawler.h indirect_cow.h \
	mkbpfs.h dcache.h util.h hash_map.h
	$(CC) $(CFLAGS) -D_FILE_OFFSET_BITS=64 -I/usr/local/include/fuse -c -o $@ $<

mkfs.bpfs.o: mkfs.bpfs.c mkbpfs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

indirect_cow.o: indirect_cow.c indirect_cow.h bpfs.h bpfs_structs.h util.h \
	hash_map.h
	$(CC) $(CFLAGS) -c -o $@ $<

crawler.o: crawler.c crawler.h bpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

mkbpfs.o: mkbpfs.c mkbpfs.h bpfs.h bpfs_structs.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

dcache.o: dcache.c dcache.h hash_map.h util.h
	$(CC) $(CFLAGS) -c -o $@ $<

vector.o: vector.c vector.h
	$(CC) $(CFLAGS) -c -o $@ $<

hash_map.o: hash_map.c hash_map.h vector.h pool.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Fucking gcc was throwing away the linker flags and the linker was throwing
# a lot of "undefined references". Hence, I moved those linker flags in $^
# Same for the rule mkfs.bpfs : ...
bpfs: bpfs.o crawler.o indirect_cow.o mkbpfs.o dcache.o hash_map.o vector.o -lfuse -luuid -lpthread
	$(CC) $(CCFLAGS) -o $@ $^

mkfs.bpfs: mkfs.bpfs.o mkbpfs.o -luuid
	$(CC) $(CCFLAGS) -o $@ $^
