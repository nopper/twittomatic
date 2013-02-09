import codecs
import sys
import ngb
import gzip
from collections import defaultdict

ngrams = defaultdict(int)
builder = ngb.NgramBuilder()

output = 0

for line in codecs.getreader("utf-8")(sys.stdin):
    line = unicode(line)
    for i in xrange(1, 6):
        for ngram in builder.find_ngrams(line, i):
            ngrams[ngram] += 1


    if len(ngrams) > 1000000:
        output += 1
        with gzip.open("ngrams-%06d.tsv.gz" % output, 'w') as out:
            for k, v in sorted(ngrams.items()):
                out.write("%s\t%s\n" % (k.encode('utf8'), v))
        ngrams.clear()

if ngrams:
    output += 1
    with gzip.open("ngrams-%06d.tsv.gz" % output, 'w') as out:
        for k, v in sorted(ngrams.items()):
            out.write("%s\t%s\n" % (k.encode('utf8'), v))
    ngrams.clear()
