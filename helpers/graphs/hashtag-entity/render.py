# encoding=utf8

"""
Simple script that takes in input the TSV annotated file and generate the graphml file
"""

import gzip
from collections import defaultdict

class Renderer(object):
    def __init__(self, options):
        self.inputfile = options.inputfile
        self.outputfile = options.outputfile
        self.skip_single = options.skip_single

        if options.blacklist:
            self.blacklist = self.load_blacklist(options.blacklist)
        else:
            self.blacklist = set()

    def load_blacklist(self, inputfile):
        titles = set()

        with open(self.inputfile, 'r') as inputfile:
            for title in inputfile:
                titles.add(title.strip())

        return titles()

    def iterate(self):
        with gzip.open(self.inputfile, 'r') as inputfile:
            prevhashtag = None
            pages = []

            for line in inputfile:
                try:
                    hashtag, wid, rho, title = line.strip().split('\t', 3)
                except:
                    hashtag, wid, rho = line.strip().split('\t', 2)
                    title = ''

                hashtag = "#" + hashtag

                if prevhashtag == hashtag:
                    pages.append((int(wid), float(rho), title))
                else:
                    if prevhashtag:
                        yield prevhashtag, pages

                    prevhashtag = hashtag
                    pages = [(int(wid), float(rho), title)]

            if prevhashtag:
                yield prevhashtag, pages

        raise StopIteration

    def run(self):
        with gzip.open(self.outputfile, 'w') as outputfile:
            for count, (hashtag, pages) in enumerate(self.iterate()):
                if self.skip_single and len(pages) <= 1:
                    continue

                counters = defaultdict(list)
                mappings = {}

                for wid, rho, title in filter(lambda x: x[2] not in self.blacklist, pages):
                    counters[wid].append(rho)
                    mappings[wid] = title

                for wid, rhos in sorted(counters.items()):
                    rhos.sort()

                    line = "%d\t%d\t%s\t%s\t%s\n" % (count + 1, wid + 100000000,
                                                     ':'.join(map(str, rhos)),
                                                     hashtag,
                                                     title)
                    outputfile.write(line)

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser(description="Read the annotation the TSV annotation file and generate the final graph")
    parser.add_option("-i", "--input", dest="inputfile",
                      help="Annotation file in tsv.gz format")
    parser.add_option("-o", "--output", dest="outputfile",
                      help="Output file")
    parser.add_option("-s", "--skip-single", dest="skip_single", action="store_true",
                      help="Skip edges with just one annotation")
    parser.add_option("-b", "--blacklist", dest="blacklist",
                      help="Specify a blacklist file containing Wikipedia pages to be ignored")

    (options, args) = parser.parse_args()

    if options.inputfile and options.outputfile:
        app = Renderer(options)
        app.run()
    else:
        parser.print_help()
