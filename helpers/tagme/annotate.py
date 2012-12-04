import sys
import gzip
import json
from json import loads
from hashtag import HashtagExtractor
from language import LanguageChecker
from annotation import AnnotationExtractor

class Annotator(object):
    def __init__(self):
        self.lang = LanguageChecker('italian')
        self.hashtag = HashtagExtractor()
        self.annotator = AnnotationExtractor()

        self.italian = 0
        self.annotated = 0

        self.total = 0
        self.requests = 0

    def run(self, inputfile, outfile):
        with gzip.open(outputfile, 'w') as output:
            with gzip.open(inputfile, 'r') as f:
                for line in f:
                    json = loads(line)

                    unstripped = json['text']
                    text = self.hashtag.sanitize(unstripped)

                    # Skip non italian tweets
                    self.total += 1

                    if not self.lang.is_valid(text):
                        continue

                    self.italian += 1

                    hts = self.hashtag.extract(json)

                    # Skip text without hashtags
                    if not hts:
                        continue

                    buff = self.annotate(unstripped, text, hts)

                    if buff:
                        output.write(buff)

                    sys.stderr.write("%d annotated of %d requested of %d italians of %d processed\r" % (self.annotated, self.requests, self.italian, self.total))
                    sys.stderr.flush()

        sys.stderr.write("%d annotated of %d requested of %d italians of %d processed\n" % (self.annotated, self.requests, self.italian, self.total))
        sys.stderr.flush()

    def annotate(self, unstripped, text, hts):
        self.requests += 1
        annotations = self.annotator.annotate(text)

        if not annotations:
            return ""

        payload = {
            "hts": hts,
            "annotations": annotations
        }

        for annotation in annotations:
            if annotation[1] == 0.5:
                print "Unstripped: ", unstripped.encode('utf-8')
                print "Stripped:", text.encode('utf-8')
                print "Annotations:", annotations
                break

        self.annotated += 1
        return json.dumps(payload) + '\n'


if __name__ == "__main__":
    inputfile = sys.argv[1]
    outputfile = sys.argv[2]

    import pdb
    import traceback
    try:
        Annotator().run(inputfile, outputfile)
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem()
