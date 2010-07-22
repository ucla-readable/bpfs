#!/usr/bin/env python

# This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
# University of California. It is distributed under the terms of version 2
# of the GNU GPL. See the file LICENSE for details.

# TODO:
# - try with different types of file systems
#   (eg many inodes or dirents or large file)
# - create dir/file cases that are not SCSP optimal (that must CoW some)?

import inspect
import getopt
import os
import subprocess
import stat
import sys
import tempfile
import time

class benchmarks:
    @staticmethod
    def all():
        for name, obj in inspect.getmembers(benchmarks):
            if inspect.isclass(obj):
                yield (name, obj)

    class empty:
        #     valid
        opt = 1
        def run(self):
            pass

    class create:
        #     dirent + ino                 + cmtime + d.ft + d.ino + valid
        opt = 4+1+2  + 8+4+4+4+4+8+8+8+3*4 + 4+4    + 1    + 8     + 1
        def run(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()

    class mkdir:
        #     dirent + ino                 + cmtime + root + nlinks + nbytes + rl + d.ft + nlinks + d.ino + valid
        opt = 4+1+2  + 8+4+4+4+4+8+8+8+3*4 + 4+4    + 8    + 4      + 8      + 2  + 1    + 4      + 8     + 1
        def run(self):
            os.mkdir(os.path.join(self.mnt, 'a'))

    class unlink_0B:
        #     dirent.ino + cmtime + valid
        opt = 8          + 8      + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class unlink_4k:
        #     dirent.ino + cmtime + valid
        opt = 8          + 8      + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            file.write('0' * 4096)
            file.close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class rmdir:
        #     nlinks + dirent.ino + cmtime + valid
        opt = 4      + 8          + 8      + 1
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
        def run(self):
            os.rmdir(os.path.join(self.mnt, 'a'))

    class rename_intra:
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class rename_inter:
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
            os.mkdir(os.path.join(self.mnt, 'b'))
            open(os.path.join(self.mnt, 'a', 'c'), 'w').close()
        def run(self):
            os.rename(os.path.join(self.mnt, 'a', 'c'),
                      os.path.join(self.mnt, 'b', 'c'))

    class rename_clobber:
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
            open(os.path.join(self.mnt, 'b'), 'w').close()
        def run(self):
            pass
            # FIXME: Gets stuck during unmount() in communicate():
            # os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class link:
        #     dirent + cmtime + nlinks + ctime + d.ft + d.ino + valid
        opt = 4+1+2  + 8      + 4      + 4     + 1    + 8     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.link(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class unlink_hardlink:
        #     dirent.ino + cmtime + nlinks + ctime + valid
        opt = 8          + 8      + 4      + 4     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
            os.link(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class chmod:
        #     mode + ctime + valid
        opt = 4    + 4     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.chmod(os.path.join(self.mnt, 'a'), stat.S_IWUSR | stat.S_IRUSR)

    class append_0B_8B:
        #     data + root + size + mtime + valid
        opt = 8    + 8    + 8    + 4     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('0' * 8)
            file.close()

    class append_8B_8B:
        #     data + size + mtime + valid
        opt = 8    + 8    + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            file.write('0' * 8)
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('0' * 8)
            file.close()

    class append_0B_4k:
        #     data + root + size + mtime + valid
        opt = 4096 + 8    + 8    + 4     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('0' * 4096)
            file.close()

    class append_2M_4k:
        #     data + nr + or + in0 + in1 + size + mtime + valid
        opt = 4096 + 8  + 8  + 8   + 8   + 8    + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(2 * 64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('0' * 4096)
            file.close()

    class write_1M_8B:
        #     data + mtime + valid
        opt = 8    + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('0' * 8)
            file.close()

    class write_1M_8B_4092:
        #     dCoW        + data + iCoW    + indir + mtime + valid
        opt = 2*4096-4096 + 4096 + 4096-16 + 2*8+8 + 4     + 1
        # extra: iCoW+16
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.seek(4096 - 4)
            file.write('0' * 8)
            file.close()

    class write_1M_16B:
        #     CoW     + indir + data  + mtime + valid
        opt = 4096-16 + 8     + 16    + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('0' * 16)
            file.close()

    class write_1M_4k:
        #     data + indir + mtime + valid
        opt = 4096 + 8     + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('0' * 4096)
            file.close()

    # FIXME: fuse breaks up the write at 4kB boundaries
    class write_1M_4k_1:
        #     CoW data    + data + indir + mtime + valid
        opt = 2*4096-4096 + 4096 + 4096  + 4     + 1
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('0' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.seek(1) # FIXME: doesn't work?
            file.write('0' * 4096)
            file.close()

    class read:
        #     mtime + valid
        opt = 4     + 1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r')
            file.read(1)
            file.close()

    class readdir:
        #     mtime + mtime + valid
        opt = 4     + 4     + 1
        def run(self):
            os.listdir(self.mnt)


class filesystem:
    def __init__(self, megabytes):
        self.img = tempfile.NamedTemporaryFile()
        # NOTE: self.mnt should not be in ~/ so that gvfs does not readdir it
        self.mnt = tempfile.mkdtemp()
        self.proc = None
        for i in range(megabytes * 64):
            self.img.write('0' * (16 * 1024))
    def __del__(self):
        if self.proc:
            self.unmount()
        os.rmdir(self.mnt)
    def format(self):
        subprocess.check_call(['./mkfs.bpfs', self.img.name], close_fds=True)
    def mount(self, pinfile=None):
        env = None
        if pinfile:
            env = os.environ
            env['PINOPTS'] = '-b true -o ' + pinfile
        self.proc = subprocess.Popen(['./bench/bpramcount',
                                      '-f', self.img.name, self.mnt],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      close_fds=True,
                                      env=env)
        while self.proc.stdout:
            line = self.proc.stdout.readline()
            if line == 'BPFS running\n':
                return
        raise NameError('Unable to start BPFS')
    def unmount(self):
        self.proc.terminate()
        output = self.proc.communicate()[0]
        self.proc = None
        for line in output.splitlines():
            if line.startswith('pin: ') and line.endswith(' bytes written to BPRAM'):
                return int(line.split()[1])
        raise NameError('BPFS failed to exit correctly')

def run(benches, profile):
    fs = filesystem(16)
    for name, clz in benches:
        pinfile = None
        if profile:
            pinfile = 'pin-' + name + '.log'
        sys.stdout.write('Benchmark ' + name + ': ')
        b = clz()
        b.mnt = fs.mnt
        fs.format()

        if hasattr(b, 'prepare'):
            fs.mount()
            b.prepare()
            fs.unmount()

        fs.mount(pinfile=pinfile)
        b.run()
        bytes = fs.unmount()

        sys.stdout.write(str(bytes) + ' bytes')
        if hasattr(b, 'opt'):
            sys.stdout.write(' (' + str(bytes - b.opt) + ')')
        print ''
        if profile:
            subprocess.check_call(['./bench/parse_bpramcount'],
                                  stdin=open(pinfile))

def usage():
    print 'Usage: ' + sys.argv[0] + ' [-h|--help] [-p] [BENCHMARK ...]'
    print '\t-p: profile each run'
    print '\tSpecifying no benchmarks runs all benchmarks'

def main():
    try:
        opts, bench_names = getopt.getopt(sys.argv[1:], 'hp', ['help'])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(1)
    profile = False
    benches = []
    for o, a in opts:
        if o == '-p':
            profile = True
        elif o in ('-h', '--help'):
            usage()
            sys.exit()
        else:
            assert False, 'unhandled option'
    if not bench_names:
        benches = benchmarks.all()
    else:
        bench_names = set(bench_names)
        for name, obj in inspect.getmembers(benchmarks):
            if inspect.isclass(obj) and name in bench_names:
                benches.append((name, obj))
    run(benches, profile)

if __name__ == '__main__':
    main()
