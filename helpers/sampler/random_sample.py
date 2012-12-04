import sys
import gzip
import json
import random
from hashtag import HashtagExtractor
from language import LanguageChecker

def main(filename, outfilename, count):
    sample = []
    checker = LanguageChecker('italian')
    hashtag = HashtagExtractor()

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

    with gzip.open(outfilename, 'w') as output:
        for jobj in sample:
            output.write(json.dumps(jobj) + "\n")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
