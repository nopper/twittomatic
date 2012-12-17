import os
import sys
import gzip
import json
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tagme'))

from hashtag import HashtagExtractor
from language import LanguageChecker

def main(filename, outfilename, count, stop_after):
    sample = []
    checker = LanguageChecker('italian')
    hashtag = HashtagExtractor()

    print "Extracting a sample of %d tweets. Stopping after %d tweets" % (count, stop_after)

    with gzip.open(filename, 'r') as input:
        for idx, line in enumerate(input):
            jobj = json.loads(line)

            # Check that the tweet is actually italian
            if not checker.is_valid(jobj['text']):
                continue

            if not hashtag.extract(jobj):
                continue

            if len(sample) < count:
                sample.append(jobj)
            else:
                r = random.randint(0, idx)

                if r < count:
                    sample[r] = jobj

            if idx >= stop_after and len(sample) >= count:
                break

    with gzip.open(outfilename, 'w') as output:
        for jobj in sample:
            output.write(json.dumps(jobj) + "\n")

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input",
                      help="Input timeline file (json.gz)")
    parser.add_option("-o", "--output", dest="output",
                      help="Output file (json.gz)")
    parser.add_option("-n", "--num-samples", dest="num_sample", type="int", default=1000,
                      help="Number of tweets to extract")
    parser.add_option("-s", "--stop-after", dest="stop_after", type="int", default=10000,
                      help="Stop after having seen a certain number of tweets")

    (options, args) = parser.parse_args()

    if options.input and options.output:
        main(options.input, options.output, options.num_sample, options.stop_after)
    else:
        parser.print_help()
