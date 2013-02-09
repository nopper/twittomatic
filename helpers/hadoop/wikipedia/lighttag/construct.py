# encoding=utf8

import sys
import gzip
import json

from utils import profiled

def extract_trie(filename='anchors/anchors.gz',
                 outfilename='anchors.trie', stopafter=sys.maxint):

    # Create a complete trie
    trie = datrie.Trie([chr(x) for x in range(1, 255)])

    with gzip.open(filename, 'r') as inputfile:
        skipped = 0

        for count, line in enumerate(inputfile):
            anchor = json.loads(line.strip())
            label = anchor['anchor'].lower()

            if not len(label.split()) <= 3 or \
               not len(label) >= 3:
                skipped += 1
                continue

            trie[label] = anchor['pages']

            if count >= stopafter:
                break

            if count % 10000 == 0:
                print "Anchors: Loaded %d, Skipped %d" % (count, skipped)

    trie.save(outfilename)
