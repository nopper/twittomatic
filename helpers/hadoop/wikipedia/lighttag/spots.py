import sys
import acora

class SpotsFinder(object):
    def __init__(self, spotfile='anchors-sorted.txt'):
        builder = acora.AcoraBuilder()

        with open(spotfile, 'r') as inputfile:
            for count, line in enumerate(inputfile):
                builder.add(line.rstrip("\n"))

        print "Building the tree"
        self.tree = builder.build()

    def findall(self, contents):
        for word, start in self.tree.findall(contents):
            yield word, start, len(word) + start

if __name__ == "__main__":
    finder = SpotsFinder()
    text = sys.argv[1]
    for word, start, end in finder.findall(text):
        print "Found spot %s start: %d end: %d" % (word, start, end)
