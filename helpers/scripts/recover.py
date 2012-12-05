"""
Simple utility that recompress corrupted files
"""

import os
import json
import gzip
import struct
import fnmatch
from tempfile import NamedTemporaryFile

def recover(dirname, filename):
    timeline = {}
    fullpath = os.path.join(dirname, filename)

    with gzip.open(fullpath, 'r') as input:
        try:
            for line in input:
                try:
                    tweet = json.loads(line.strip())
                    timeline[int(tweet['id_str'])] = tweet
                except:
                    continue
        except:
            print "File %s is corrupted" % filename

    with NamedTemporaryFile(prefix='recover-',
                            suffix='.twt.rec',
                            dir=dirname,
                            delete=False) as outfile:
        with gzip.GzipFile(mode='w', fileobj=outfile) as gzfile:
            for k, tweet in sorted(timeline.items(), reverse=True):
                gzfile.write(json.dumps(tweet, sort_keys=True) + '\n')

        outfile.close()
        os.rename(os.path.join(dirname, outfile.name),
                  os.path.join(dirname, filename))

    print "File %s with %d tweets" % (filename, len(timeline))
    return len(timeline)



def collect_files(directory):
    count = 0
    tweet_count = 0

    dirname = os.path.abspath(directory)

    for root, dirnames, filenames in os.walk(dirname):
        for filename in fnmatch.filter(filenames, '*.twt'):
            tweet_count += recover(root, filename)
            count += 1

    print "%d files reconstructed, %d total tweets" % (count, tweet_count)

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(description="Recover corrupted timeline files")
    parser.add_option("-d", "--dataset", action="store", type="string", dest="dataset")

    (options, args) = parser.parse_args()

    if options.dataset:
        collect_files(options.dataset)
    else:
        parser.print_help()