import os
import gzip
import shutil
import datetime
from tempfile import TemporaryFile, NamedTemporaryFile

from contextlib import contextmanager
from twitter.settings import *
from twisted.python import log

if USE_HDFS:
    from pydoop import hdfs

    hdfs_handle = hdfs.fs.hdfs()

def get_filename(user_id, extension, create=True, hdfs_dest=False):
    if hdfs_dest:
        return os.path.join(HDFS_DIRECTORY, str(user_id)[:2], "%s.%s" % (user_id, extension))

    dirname = os.path.join(OUTPUT_DIRECTORY, str(user_id)[:2])

    if create and not os.path.exists(dirname):
        try:
            os.mkdir(dirname)
        except:
            pass

    return os.path.join(dirname, "%s.%s" % (user_id, extension))

def copy_contents(user_id, extension, dest):
    filename = get_filename(user_id, extension, create=False)

    if os.path.exists(filename):
        with open(filename, 'r') as srcfile:
            shutil.copyfileobj(srcfile, dstfile)

@contextmanager
def profiled(str):
    start = datetime.datetime.now()
    yield start
    diff = datetime.datetime.now() - start
    log.msg(str % diff)

class StatsFile(object):
    def __init__(self):
        self.abort = False

def copy_contents(srcfileobj, dstfileobj):
    return shutil.copyfileobj(srcfileobj, dstfileobj)

def new_tempfile():
    return TemporaryFile(prefix='twitter-')

def commit_file_compressed(srcfile, user_id, extension):
    dstfilename = get_filename(user_id, extension, create=True, hdfs_dest=USE_HDFS)

    with profiled("Uploading of output in %s"):
        # Atomic rename on POSIX
        log.msg("Renaming %s to %s" % (srcfile.name, dstfilename))
        srcfile.close()

        # Race condition here?
        if USE_HDFS:

            if hdfs.path.exists(dstfilename):
                if hdfs.path.exists(dstfilename + '.new'):
                    log.msg("Apparently a crashed worker left an unused file left")
                    hdfs_handle.delete(dstfilename + '.new')

                hdfs.put(srcfile.name, dstfilename + '.new')
                hdfs_handle.delete(dstfilename)
                hdfs_handle.rename(dstfilename + '.new', dstfilename)
            else:
                hdfs.put(srcfile.name, dstfilename)

            os.unlink(srcfile.name)
        else:
            os.rename(srcfile.name, dstfilename)

def commit_file(srcfile, user_id, extension):
    # We need to copy the file contents to the original location
    compfile = NamedTemporaryFile(prefix='twitter-', dir=TEMPORARY_DIRECTORY, delete=False)

    with profiled("Compressing output in %s"):
        with gzip.GzipFile(mode='wb', fileobj=compfile) as gzfile:
            srcfile.seek(0)
            shutil.copyfileobj(srcfile, gzfile)
            log.msg("Output file size is %d bytes (%d bytes compressed)" % (gzfile.tell(), compfile.tell()))

        srcfile.close() # Delete the old plain file
        compfile.close()

    commit_file_compressed(compfile, user_id, extension)


APPEND = 1
READ   = 2
WRITE  = 4

@contextmanager
def local_copy(user_id, extension, mode):
    """
    TODO: if the file is only opened in read mode as in followers just avoid all this shit
    """

    # If we are just reading the file do not create an additional copy
    if mode == READ and not USE_HDFS:
        srcfilename = get_filename(user_id, extension, create=False)
        with gzip.open(srcfilename, 'rb') as srcfile:
            yield srcfile, StatsFile()

        return

    with profiled("Downloading local copy in %s"):
        dstfile = TemporaryFile(prefix='twitter-')
        srcfilename = get_filename(user_id, extension, create=False, hdfs_dest=USE_HDFS)

        if USE_HDFS:
            exists = hdfs.path.exists
            open_file = hdfs.open
        else:
            exists = os.path.exists
            open_file = open

        if exists(srcfilename):
            with open_file(srcfilename) as filehandle:
                with gzip.GzipFile('rb', fileobj=filehandle) as srcfile:
                    shutil.copyfileobj(srcfile, dstfile)
                    log.msg("Original decompressed file size is %d bytes" % srcfile.tell())

    # A flag indicating whether is compressed or not would be better
    if mode & READ or mode & WRITE:
        dstfile.seek(0)

    stats = StatsFile()
    yield dstfile, stats

    if mode == READ and USE_HDFS:
        return

    dstfile.flush()

    if stats.abort:
        log.msg("Aborting merge as application requested")
        dstfile.close()
        return

    if mode & APPEND or mode & WRITE:
        commit_file(dstfile, user_id, extension)
    else:
        dstfile.close()

open_file = local_copy
