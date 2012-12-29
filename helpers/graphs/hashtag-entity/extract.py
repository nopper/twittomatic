"""
Scripts that read annotation file and outputs a tab-separated stream

Use it in conjunction of sort -k1 -u
"""

import sys
import json
import gzip

def read(inputfile):
    with gzip.open(inputfile, 'r') as input:
        for line in input:
            obj = json.loads(line.strip())

            hts = obj['hts']
            annotations = obj['annotations']

            for ht in sorted(hts):
                for annotation in sorted(annotations):
                    line = "%s\t%s\t%s\t%s" % (ht, annotation[0], annotation[1], annotation[2])
                    print line.encode('utf8')

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser(description="Read the annotation file and outputs TSV")
    parser.add_option("-i", "--input", dest="inputfile",
                      help="Annotation file in json.gz format")

    (options, args) = parser.parse_args()

    if options.inputfile:
        read(options.inputfile)
    else:
        parser.print_help()
