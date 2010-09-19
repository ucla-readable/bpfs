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

def benchmacro(bench_class):
    bench_class.benchmacro = True
    return bench_class

class postmark:
    free_space = 6 * 1024
    def run(self):
        config = tempfile.NamedTemporaryFile()
        config.write('set location ' + self.mnt + '\n')
        defaults = open(self.config)
        for line in defaults:
            config.write(line)
        defaults.close()
        config.flush()
        config.seek(0)
        subprocess.check_call(['bench/postmark-1_5'],
                              stdin=config, close_fds=True)

class benchmarks:
    @staticmethod
    def all():
        for name, obj in inspect.getmembers(benchmarks):
            if inspect.isclass(obj):
                yield (name, obj)

    @staticmethod
    def micro():
        for (name, obj) in benchmarks.all():
            if not hasattr(obj, 'benchmacro'):
                yield (name, obj)

    @staticmethod
    def macro():
        for (name, obj) in benchmarks.all():
            if hasattr(obj, 'benchmacro'):
                yield (name, obj)

    class empty:
        opt = 0
        def run(self):
            pass

    class create:
        #     dirent + ino                 + cmtime + d.ft + d.ino
        opt = 4+1+2  + 8+4+4+4+4+8+8+8+3*4 + 4+4    + 1    + 8
        def run(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()

    class mkdir:
        #     dirent + ino                 + cmtime + d.ft + d.nlinks + d.ino + dirent.rec_len
        opt = 4+1+2  + 8+4+4+4+4+8+8+8+3*4 + 4+4    + 1    + 4        + 8     + 2
        def run(self):
            os.mkdir(os.path.join(self.mnt, 'a'))

    class symlink:
        #     dirent + ino                 + cmtime + d.ft + d.ino + filename
        opt = 4+1+2  + 8+4+4+4+4+8+8+8+3*4 + 4+4    + 1    + 8     + 2
        def run(self):
            os.symlink('a', os.path.join(self.mnt, 'b'))

    class unlink_0B:
        #     dirent.ino + cmtime
        opt = 8          + 8
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class unlink_4k:
        #     dirent.ino + cmtime
        opt = 8          + 8
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            file.write('1' * 4096)
            file.close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class unlink_1M:
        #     dirent.ino + cmtime
        opt = 8          + 8
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(1 * 64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class unlink_16M:
        #     dirent.ino + cmtime
        opt = 8          + 8
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(16 * 64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class rmdir:
        #     nlinks + dirent.ino + cmtime
        opt = 4      + 8          + 8
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
        def run(self):
            os.rmdir(os.path.join(self.mnt, 'a'))

    class unlink_symlink:
        #     dirent.ino + cmtime
        opt = 8          + 8
        def prepare(self):
            os.symlink('a', os.path.join(self.mnt, 'b'))
        def run(self):
            os.unlink(os.path.join(self.mnt, 'b'))

    class rename_file_intra:
        # TODO: could reduce dirent block by 2*8 and by unused
        #     inos + dirents + ino_root + cmtime + rec_len + dirent
        opt = 2*8  + 4096    + 8        + 2*4    + 2       + 2+1+2+1
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class rename_file_inter:
        # TODO: could reduce dirent blocks by 2*8 and by unused
        # TODO: could reduce ino_roots by 2*8 and by unused
        #     inos + dirents + ino_roots+ ira + cmtime + rec_len + dirent
        #                                (ira = root_inode addr)
        opt = 2*8  + 2*4096  + 4096+2*8 + 8   + 4*4    + 2       + 2+1+2+1
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
            os.mkdir(os.path.join(self.mnt, 'b'))
            open(os.path.join(self.mnt, 'a', 'c'), 'w').close()
        def run(self):
            os.rename(os.path.join(self.mnt, 'a', 'c'),
                      os.path.join(self.mnt, 'b', 'c'))

    class rename_file_clobber:
        # TODO: could reduce dirent blocks by 2*8 and by unused
        # TODO: could reduce ino_roots by 2*8 and by unused
        #     inos + dirents + ino_root + cmtime
        opt = 2*8  + 4096    + 8        + 2*4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
            open(os.path.join(self.mnt, 'b'), 'w').close()
        def run(self):
            os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class rename_dir_intra:
        # TODO: could reduce dirent block by 2*8 and by unused
        #     inos + dirents + ino_root + cmtime + rec_len + dirent  + child ctime
        opt = 2*8  + 4096    + 8        + 2*4    + 2       + 2+1+2+1 + 4
        # over file, has 3x4B callback_crawl_inode calls: nlinks [on]p, ctime
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
        def run(self):
            os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class rename_dir_inter:
        # TODO: could reduce dirent blocks by 2*8 and by unused
        # TODO: could reduce ino_roots by 2*8 and by unused
        #     inos + dirents + ino_roots+ ira + cmtime + rec_len + dirent  + old and new parent nlinks + child ctime
        #                                (ira = root_inode addr)
        opt = 2*8  + 2*4096  + 4096+2*8 + 8   + 4*4    + 2       + 2+1+2+1 + 2*4                       + 4
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
            os.mkdir(os.path.join(self.mnt, 'b'))
            os.mkdir(os.path.join(self.mnt, 'a', 'c'))
        def run(self):
            os.rename(os.path.join(self.mnt, 'a', 'c'),
                      os.path.join(self.mnt, 'b', 'c'))

    class rename_dir_clobber:
        # TODO: could reduce dirent blocks by 2*8 and by unused
        # TODO: could reduce ino_roots by 2*8 and by unused
        #     inos + dirents + ino_root + cmtime + old parent nlinks + child ctime
        opt = 2*8  + 4096    + 8        + 2*4    + 4                 + 4
        def prepare(self):
            os.mkdir(os.path.join(self.mnt, 'a'))
            os.mkdir(os.path.join(self.mnt, 'b'))
        def run(self):
            os.rename(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class link:
        #     dirent + cmtime + nlinks + ctime + d.ft + d.ino
        opt = 4+1+2  + 8      + 4      + 4     + 1    + 8
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.link(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))

    class unlink_hardlink:
        #     dirent.ino + cmtime + nlinks + ctime
        opt = 8          + 8      + 4      + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
            os.link(os.path.join(self.mnt, 'a'), os.path.join(self.mnt, 'b'))
        def run(self):
            os.unlink(os.path.join(self.mnt, 'a'))

    class chmod:
        #     mode + ctime
        opt = 4    + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.chmod(os.path.join(self.mnt, 'a'), stat.S_IWUSR | stat.S_IRUSR)

    class chown:
        #     uid + gid + ctime
        opt = 4   + 4   + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            os.chown(os.path.join(self.mnt, 'a'), 0, 0)

    class append_0B_8B:
        #     data + root + size + mtime
        opt = 8    + 8    + 8    + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * 8)
            file.close()

    class append_8B_8B:
        #     data + size + mtime
        opt = 8    + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            file.write('1' * 8)
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * 8)
            file.close()

    class append_0B_4k:
        #     data + root + size + mtime
        opt = 4096 + 8    + 8    + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * 4096)
            file.close()

    class append_8k_4k:
        #     data + root + size + mtime
        opt = 4096 + 8    + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            file.write('1' * (8 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * 4096)
            file.close()

    # 128kiB is the largest that FUSE will atomically write
    class append_0B_128k:
        # TODO: changing height separately from root is needless
        #     data     + indir   + height + root + size + mtime
        opt = 128*1024 + 128/4*8 + 8      + 8    + 8    + 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * (128 * 1024))
            file.close()

    class append_2M_4k:
        #     data + nr + or + in0 + in1 + size + mtime
        opt = 4096 + 8  + 8  + 8   + 8   + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(2 * 64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * 4096)
            file.close()

    # 128kiB is the largest that FUSE will atomically write
    class append_2M_128k:
        #     data     + indir1  + indir0 + root addr/height + size + mtime
        opt = 128*1024 + 128/4*8 + 2*8    + 8                + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(2 * 64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'a')
            file.write('2' * (128 * 1024))
            file.close()

    class write_1M_8B:
        #     data + mtime
        opt = 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('2' * 8)
            file.close()

    class write_1M_8B_4092:
        #     dCoW     + data + iCoW  + indir + mtime
        opt = 2*4096-8 + 8    + 4096  + 2*8+8 + 4
        # extra: iCoW+16
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.seek(4096 - 4)
            file.write('2' * 8)
            file.close()

    class write_1M_16B:
        #     CoW     + indir + data  + mtime
        opt = 4096-16 + 8     + 16    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('2' * 16)
            file.close()

    class write_1M_4k:
        #     data + indir + mtime
        opt = 4096 + 8     + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('2' * 4096)
            file.close()

    class write_1M_4k_1:
        #     CoW data    + data + indir      + mtime
        opt = 2*4096-4096 + 4096 + 4096+2*8+8 + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.seek(1)
            file.write('2' * 4096)
            file.close()

    # 128kiB is the largest that FUSE will atomically write
    class write_1M_128k:
        # TODO: avoid CoWing indir slots that will be overwritten
        #     data     + indir   + iCoW + root + mtime
        opt = 128*1024 + 128/4*8 + 4096 + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.write('2' * (128 * 1024))
            file.close()

    # 128kiB is the largest that FUSE will atomically write
    class write_1M_124k_1:
        # TODO: avoid CoWing indir slots that will be overwritten
        #     dCoW   + data     + indir   + iCoW + root + mtime
        opt = 1+4095 + 124*1024 + 128/4*8 + 4096 + 8    + 4
        def prepare(self):
            file = open(os.path.join(self.mnt, 'a'), 'w')
            for i in range(64):
                file.write('1' * (16 * 1024))
            file.close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r+', 0)
            file.seek(1)
            file.write('2' * (124 * 1024))
            file.close()

    class read:
        #     atime
        opt = 4
        def prepare(self):
            open(os.path.join(self.mnt, 'a'), 'w').close()
        def run(self):
            file = open(os.path.join(self.mnt, 'a'), 'r')
            file.read(1)
            file.close()

    class readdir:
        #     atime + atime
        opt = 4     + 4
        def run(self):
            os.listdir(self.mnt)

    @benchmacro
    class postmark_small(postmark):
        config = 'bench/postmark.small.config'

    @benchmacro
    class postmark_large(postmark):
        config = 'bench/postmark.large.config'

    @benchmacro
    class tarx:
        free_space = 512
        def run(self):
            tar_file = 'bench/linux-2.6.15.tar'
            subprocess.check_call(['tar', '-xf', tar_file, '-C', self.mnt],
                                  close_fds=True)

    @benchmacro
    class delete:
        free_space = 512
        def prepare(self):
            tar_file = 'bench/linux-2.6.15.tar'
            subprocess.check_call(['tar', '-xf', tar_file, '-C', self.mnt],
                                  close_fds=True)
        def run(self):
            subprocess.check_call(['rm', '-rf',
                                   os.path.join(self.mnt, 'linux-2.6.15')],
                                   close_fds=True)

    @benchmacro
    class build_apache:
        free_space = 6 * 1024
        def run(self):
            tar_file = 'bench/httpd-2.0.63.tar.gz'
            path = os.path.join(self.mnt, 'httpd-2.0.63')
            devnull = open('/dev/null', 'rw')
            subprocess.check_call(['tar', '-xf', tar_file, '-C', self.mnt],
                                  close_fds=True)
            subprocess.check_call([os.path.join(path, 'configure')],
                                  stdout=devnull, stderr=devnull,
                                  cwd=path, close_fds=True)
            # TODO: why does make exit with an error with devnull?
            # 'make -j8 &>/dev/null' does not.
            subprocess.check_call(['make', '-j8'],
#                                  stdout=devnull, stderr=devnull,
                                  cwd=path, close_fds=True)
            devnull.close()

    @benchmacro
    class bonnie:
        free_space = 6 * 1024
        def run(self):
            cmd = ['bonnie++', '-d', self.mnt, '-r', '1024']
            devnull = open('/dev/null', 'rw')
            subprocess.check_call(cmd,
                                  stdout=devnull, stderr=devnull,
                                  close_fds=True)
            devnull.close()

    @benchmacro
    class bonnie_sync:
        free_space = 6 * 1024
        def run(self):
            cmd = ['bonnie++', '-d', self.mnt, '-r', '1024', '-b']
            devnull = open('/dev/null', 'rw')
            subprocess.check_call(cmd,
                                  stdout=devnull, stderr=devnull,
                                  close_fds=True)
            devnull.close()


class filesystem_bpfs:
    _mount_overheads = { 'BPFS': 1 } # the valid field
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
    def mount(self, pinfile=None, count=False):
        env = None
        if pinfile:
            env = os.environ
            env['PINOPTS'] = '-b true -o ' + pinfile
        bin = './bpfs'
        if count:
            bin = './bench/bpramcount'
        self._count = count
        self.proc = subprocess.Popen([bin, '-f', self.img.name, self.mnt],
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT,
                                      close_fds=True,
                                      env=env)
        while self.proc.stdout:
            line = self.proc.stdout.readline()
            if line.startswith('BPFS running'):
                self._commit_mode = line.split()[3]
                return
        raise NameError('Unable to start BPFS')
    def unmount(self):
        cowed = (-1, -1)
        # 'fusermount -u' rather than self.proc.terminate() because the
        # second does not always get its signal into the process.
        # (In particular for benchmarks.rename_clober when running
        # all benchmarks. This behavior seems to come and go.)
        subprocess.check_call(['fusermount', '-u', self.mnt], close_fds=True)
        output = self.proc.communicate()[0]
        self.proc = None
        if not self._count:
            return 0
        for line in output.splitlines():
            if line.startswith('CoW: ') and line.endswith(' blocks'):
                linea = line.split()
                cowed = (int(linea[1]), int(linea[4]))
            if line.startswith('pin: ') and line.endswith(' bytes written to BPRAM'):
                bytes_written = int(line.split()[1])
                if self._commit_mode in self._mount_overheads:
                    bytes_written -= self._mount_overheads[self._commit_mode]
                return (bytes_written, cowed)
        raise NameError('BPFS failed to exit correctly')

class filesystem_kernel:
    def __init__(self, fs_name, img):
        fs_full = fs_name.split('-')
        if len(fs_full) > 1:
            self.fs_name = fs_full[0]
            self.fs_mode = fs_full[1]
        else:
            self.fs_name = fs_name
            self.fs_mode = None
        self.img = img
        # NOTE: self.mnt should not be in ~/ so that gvfs does not readdir it
        self.mnt = tempfile.mkdtemp()
        self.mounted = False
    def __del__(self):
        if self.mounted:
            self.unmount()
        os.rmdir(self.mnt)
    def format(self):
        cmd = ['sudo', 'mkfs.' + self.fs_name, self.img]
        if self.fs_name in ['ext2', 'ext3', 'ext4']:
            cmd.append('-q')
        subprocess.check_call(cmd, close_fds=True)
    def _get_dev_writes(self):
        dev_name = os.path.basename(self.img)
        file = open('/proc/diskstats', 'r')
        for line in file:
            fields = line.split()
            if fields[2] == dev_name:
                return int(fields[9]) * 512
        raise NameError('Device ' + dev_name + ' not found in /proc/diskstats')
    def mount(self, pinfile=None, count=False):
        cmd = ['sudo', 'mount', self.img, self.mnt]
        if self.fs_mode and self.fs_name in ['ext3', 'ext4']:
                cmd.append('-o')
                cmd.append('data=' + self.fs_mode)
        subprocess.check_call(cmd, close_fds=True)
        self.mounted = True
        subprocess.check_call(['sudo', 'chmod', '777', self.mnt],
                              close_fds=True)
        # Try to ignore the format and mount in write stats:
        subprocess.check_call(['sync'], close_fds=True)
        self.start_bytes = self._get_dev_writes()
    def unmount(self):
        # Catch all fs activity in write stats:
        subprocess.check_call(['sync'], close_fds=True)
        # Get write number before unmount to avoid including its activity
        stop_bytes = self._get_dev_writes()
        subprocess.check_call(['sudo', 'umount', self.mnt],
                              close_fds=True)
        self.mounted = False
        return (stop_bytes - self.start_bytes, (-1, -1))

def run(fs, benches, profile):
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

        fs.mount(pinfile=pinfile, count=True)
        b.run()
        (bytes, (cow_bytes, cow_blocks)) = fs.unmount()

        sys.stdout.write(str(bytes) + ' bytes')
        if hasattr(b, 'opt'):
            delta = bytes - b.opt
            delta = '%+d' % delta
            sys.stdout.write(' (' + delta + ' bytes')
            if b.opt:
                factor = float(delta) / float(b.opt)
                factor = '%+.2f' % factor
                sys.stdout.write(' = ' + factor + 'x')
            sys.stdout.write(')')
        if cow_bytes != -1:
            sys.stdout.write(' (cow: ' + str(cow_bytes) + ' bytes in ' + str(cow_blocks) + ' blocks)')
        print ''
        if profile:
            #subprocess.check_call(['cat'], stdin=open(pinfile))
            subprocess.check_call(['./bench/parse_bpramcount'],
                                  stdin=open(pinfile))
            os.unlink(pinfile)
        sys.stdout.flush()

def usage():
    print 'Usage: ' + sys.argv[0] + ' [-h|--help] [-t FS [-d DEV]] [-p] [BENCHMARK ...]'
    print '\t-t FS: use file system FS (e.g., bpfs or ext4)'
    print '\t-d DEV: use DEV for (non-bpfs) file system backing'
    print '\t-p: profile each run (bpfs only)'
    print '\tThree meta benchmark names exist: all, micro, and macro'
    print '\tSpecifying no benchmarks runs all micro benchmarks'

def main():
    try:
        opts, bench_names = getopt.getopt(sys.argv[1:], 'hpt:d:', ['help'])
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(1)
    profile = False
    benches = []
    fs_name = 'bpfs'
    dev = None
    fs = None
    for o, a in opts:
        if o == '-t':
            fs_name = a
        elif o == '-d':
            dev = a
        elif o == '-p':
            profile = True
        elif o in ('-h', '--help'):
            usage()
            sys.exit()
        else:
            assert False, 'unhandled option'

    if not bench_names:
        benches = list(benchmarks.micro())
    else:
        all_benches = dict(benchmarks.all())
        for name in bench_names:
            if name in all_benches:
                benches.append((name, all_benches[name]))
            elif name == 'all':
                benches.extend(benchmarks.all())
            elif name == 'micro':
                benches.extend(benchmarks.micro())
            elif name == 'macro':
                benches.extend(benchmarks.macro())
            else:
                print '"%s" is not a benchmark' % name

    if fs_name == 'bpfs':
        bpfs_size = 32
        for (name, obj) in benches:
            if hasattr(obj, 'free_space'):
                bpfs_size = max(bpfs_size, obj.free_space)
        fs = filesystem_bpfs(bpfs_size)
    else:
        if dev == None:
            raise NameError('Must provide a backing device for ' + fs_name)
        fs = filesystem_kernel(fs_name, dev)

    run(fs, benches, profile)


if __name__ == '__main__':
    main()
