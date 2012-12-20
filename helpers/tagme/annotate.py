import sys
import gzip
import json
from json import loads
from hashtag import HashtagExtractor
from language import LanguageChecker
from annotation import AnnotationExtractor

class Annotator(object):
    def __init__(self, input_file, output_file, rho_log, ht_log):
        self.lang = LanguageChecker('italian')
        self.hashtag = HashtagExtractor()
        self.annotator = AnnotationExtractor()

        self.italian = 0
        self.annotated = 0

        self.total = 0
        self.requests = 0

        self.coht = 0
        self.rho_warn = 0

        self.rho_log = gzip.open(rho_log, 'w')
        self.ht_log = gzip.open(ht_log, 'w')

        self.input_file = input_file
        self.output_file = output_file

    def run(self):
        with gzip.open(self.output_file, 'w') as output:
            with gzip.open(self.input_file, 'r') as f:
                for line in f:
                    json = loads(line)

                    unstripped = json['text']
                    tweet_id = json['id_str']
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

                    buff = self.annotate(tweet_id, unstripped, text, hts)

                    if buff:
                        output.write(buff)

                    if self.annotated % 1000 == 0:
                        sys.stderr.write("%d annotated of %d requested of %d italians of %d processed [%d warning, %d co-ht]\r" % (self.annotated, self.requests, self.italian, self.total, self.rho_warn, self.coht))
                        sys.stderr.flush()

        sys.stderr.write("%d annotated of %d requested of %d italians of %d processed [%d warning, %d co-ht]\n" % (self.annotated, self.requests, self.italian, self.total, self.rho_warn, self.coht))
        sys.stderr.flush()

    def annotate(self, tweet_id, unstripped, text, hts):
        self.requests += 1
        annotations = self.annotator.annotate(text)

        if not annotations:
            return ""

        payload = {
            "hts": hts,
            "annotations": annotations,
            "id": tweet_id,
            "tweet": text
        }

        self.annotated += 1
        buff = json.dumps(payload) + '\n'

        for annotation in annotations:
            if annotation[1] == 0.5:
                self.rho_log.write(buff)
                self.rho_warn += 1
                break

        if len(hts) >= 2:
            self.ht_log.write(json.dumps(hts) + '\n')
            self.coht += 1

        return buff


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-i", "--input", dest="input",
                      help="Input timeline file (json.gz)")
    parser.add_option("-o", "--output", dest="output",
                      help="Output file annotated file (json.gz)")
    parser.add_option("-e", "--epsilon", dest="epsilon", type="float", default=0.4,
                      help="Epsilon option for TagME (default: 0.4)")
    parser.add_option("--log-05rho", dest="rho_log",
                      help="Output file for 0.5 rho annotations (json.gz)")
    parser.add_option("--log-coht", dest="ht_log",
                      help="Output file for co-occurring hashtag (json.gz)")

    (options, args) = parser.parse_args()

    if options.input and options.output and options.rho_log and options.ht_log:
        app = Annotator(options.input, options.output, options.rho_log, options.ht_log)
        app.run()
    else:
        parser.print_help()