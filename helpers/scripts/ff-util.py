"""
Simple utility to deal with follower files.
"""

import sys
import gzip
import struct
from optparse import OptionParser

def convert(filename):
    count = 0
    with gzip.open(filename, 'r') as input:
        with gzip.open(filename + '.new', 'w') as output:

            for line in input:
                try:
                    follower_id = int(line.strip())
                    output.write(struct.pack("!Q", follower_id))
                    count += 1
                except:
                    continue

    print "Successfully converted %s to the new format (%d followers)" % (filename, count)

def cat(filename):
    with gzip.open(filename, 'r') as input:
        while True:
            data = input.read(struct.calcsize("!Q"))
            if not data:
                break
            print struct.unpack("!Q", data)[0]

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--cat", action="store_true", dest="cat",
                      help="Cat the contents of the follower file")
    parser.add_option("--convert", action="store_true", dest="convert",
                      help="Convert the old version of the file to the new version")

    (options, args) = parser.parse_args()

    if options.cat:
        cat(args[0])
    elif options.convert:
        convert(args[0])
    else:
        parser.print_help()
