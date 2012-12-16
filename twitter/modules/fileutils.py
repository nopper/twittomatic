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

@contextmanager
def profiled(str):
    start = datetime.datetime.now()
    yield start
    diff = datetime.datetime.now() - start
    log.msg(str % diff)

def copy_contents(srcfileobj, dstfileobj):
    return shutil.copyfileobj(srcfileobj, dstfileobj)

@contextmanager
def decompressor(fileobj):
    if USE_COMPRESSION:
        with gzip.GzipFile('rb', fileobj=fileobj) as handle:
            yield handle
    else:
        yield fileobj

@contextmanager
def compressor(fileobj):
    if USE_COMPRESSION:
        with gzip.GzipFile(mode='wb', fileobj=fileobj) as handle:
            yield handle
    else:
        yield fileobj

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
    if USE_COMPRESSION:
        extension += '.gz'

    # We need to copy the file contents to the original location
    compfile = NamedTemporaryFile(prefix='twitter-', dir=TEMPORARY_DIRECTORY, delete=False)

    with profiled("Compressing output in %s"):
        with compressor(compfile) as gzfile:
            srcfile.seek(0)
            shutil.copyfileobj(srcfile, gzfile)
            log.msg("Output file size is %d bytes (%d bytes compressed)" % (gzfile.tell(), compfile.tell()))

        srcfile.close() # Delete the old plain file
        compfile.close()

    commit_file_compressed(compfile, user_id, extension)

def download(user_id, extension):
    if USE_COMPRESSION:
        extension += '.gz'

    with profiled("Downloading local copy in %s"):
        dstfile = TemporaryFile(prefix='twitter-', suffix='.' + extension)
        srcfilename = get_filename(user_id, extension, create=False, hdfs_dest=USE_HDFS)

        if USE_HDFS:
            exists = hdfs.path.exists
            open_file = hdfs.open
        else:
            exists = os.path.exists
            open_file = open

        if exists(srcfilename):
            with open_file(srcfilename) as filehandle:
                with decompressor(filehandle) as srcfile:
                    shutil.copyfileobj(srcfile, dstfile)
                    log.msg("Original decompressed file size is %d bytes" % srcfile.tell())

        return dstfile

def new_tempfile(user_id, extension):
    return TemporaryFile(prefix='twitter-', suffix='.' + extension)
