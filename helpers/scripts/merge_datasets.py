"""
Simple scripts to merge two datasets together
"""

import os
import json
import gzip
import struct
import fnmatch

def get_filename(user_id, extension, directory, create=False):
    dirname = os.path.join(os.path.expanduser(directory), str(user_id)[:2])

    if create and not os.path.exists(dirname):
        try:
            os.mkdir(dirname)
        except:
            pass

    return os.path.join(dirname, "%s.%s" % (user_id, extension))

def collect_files(directory):
    matches = set()
    for root, dirnames, filenames in os.walk(os.path.expanduser(directory)):
        for filename in fnmatch.filter(filenames, '*.twt'):
            try:
                user_id = int(os.path.basename(filename).replace('.twt', ''))
                matches.add(user_id)
            except:
                pass
    return matches

def add_tweets_into(dct, fileobj):
    try:
        for line in fileobj:
            tweet = json.loads(line.strip())
            dct[int(tweet['id_str'])] = tweet
    except IOError:
        print "File %s is corrupted" % fileobj.name

    return dct

def merge_timelines(user_id, dir1, dir2, destdir, dry_run):
    file1 = get_filename(user_id, 'twt', dir1)
    file2 = get_filename(user_id, 'twt', dir2)
    contents = {}

    if os.path.exists(file1) and os.path.exists(file2):
        with gzip.open(file1, 'r') as fo1:
            add_tweets_into(contents, fo1)
        with gzip.open(file2, 'r') as fo2:
            add_tweets_into(contents, fo2)

    if contents:
        destfile = get_filename(user_id, 'twt', destdir, create=True)

        if not dry_run:
            with gzip.open(destfile, 'w') as output:
                for count, (id_str, tweet) in enumerate(sorted(contents.items(), reverse=True)):
                    output.write(json.dumps(tweet, sort_keys=True) + "\n")

        print "Merged %s and %s into %s [%d tweets]" % (file1, file2, destfile, len(contents))

def merge_followers(user_id, dir1, dir2, destdir, dry_run):
    file1 = get_filename(user_id, 'fws', dir1)
    file2 = get_filename(user_id, 'fws', dir2)
    followers = set()

    if os.path.exists(file1) and os.path.exists(file2):
        with gzip.open(file1, 'r') as fo1:
            while True:
                data = fo1.read(struct.calcsize("!Q"))
                if not data:
                    break
                followers.add(struct.unpack("!Q", data)[0])

        with gzip.open(file2, 'r') as fo2:
            while True:
                data = fo2.read(struct.calcsize("!Q"))
                if not data:
                    break
                followers.add(struct.unpack("!Q", data)[0])

    if followers:
        destfile = get_filename(user_id, 'fws', destdir, create=True)

        if not dry_run:
            with gzip.open(destfile, 'w') as output:
                for follower in followers:
                    output.write(struct.pack("!Q", follower))

        print "Merged %s and %s into %s [%d followers]" % (file1, file2, destfile, len(followers))

def merge_files_in(files, dir1, dir2, destdir, dry_run):
    for user_id in files:
        merge_timelines(user_id, dir1, dir2, destdir, dry_run)
        merge_followers(user_id, dir1, dir2, destdir, dry_run)

def merge_datasets(dir1, dir2, destdir, dry_run):
    # Here we could optimize to just consider new files if we have an intersection
    # but it's pretty useless on small dataset
    files1 = collect_files(dir1)
    print "First directory contains %d profiles" % len(files1)
    files2 = collect_files(dir2)
    print "Second directory containts %d profiles" % len(files2)
    tomerge = files1.intersection(files2)
    print "%d users involved in the merge" % len(tomerge)
    merge_files_in(tomerge, dir1, dir2, destdir, dry_run)

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--dir1", dest="dir1",
                      help="Directory number 1.")
    parser.add_option("--dir2", dest="dir2",
                      help="Directory number 2. Usually the one containing updates")
    parser.add_option("--dst", dest="dst",
                      help="Destination directory. Usually the same as first directory")
    parser.add_option("--dry-run", action="store_true", dest="dry_run",
                      help="Don't execute any modification. Just a dry-run")

    (options, args) = parser.parse_args()

    if options.dir1 and options.dir2 and options.dst:
        merge_datasets(options.dir1, options.dir2, options.dst, options.dry_run)
    else:
        parser.print_help()
