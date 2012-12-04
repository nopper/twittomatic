import os
import sys
import gzip
import json
import requests
from language import LanguageChecker

SAMPLE_URL = "https://stream.twitter.com/1.1/statuses/sample.json"

class StreamMonitor(object):
    def __init__(self, username, password, output):
        self.auth = (username, password)
        self.output = output
        self.checker = LanguageChecker('italian')

    def run(self):
        r = requests.get(SAMPLE_URL, auth=self.auth, prefetch=False)

        with gzip.open(self.output, 'a') as output:
            for line in r.iter_lines():
                if not line:
                    continue

                tweet = json.loads(line)

                if 'text' in tweet and \
                   tweet['user']['lang'] == 'it' and \
                   self.checker.is_valid(tweet['text']) and \
                   len(tweet['entities']['hashtags']) > 1:
                    output.write(line + "\n")
                    print tweet['text']


if __name__ == "__main__":
    username, password = os.getenv('TWITTER_AUTH').rsplit(':', 1)
    StreamMonitor(username, password, sys.argv[1]).run()
