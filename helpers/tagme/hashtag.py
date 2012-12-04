# encoding=utf8
import re
from twitter_text import Extractor

class HashtagExtractor(object):
    def __init__(self, stopwords=None):
        if not stopwords:
            self.stopwords = set(['rt', 'RT', 'Rt'])
        else:
            self.stopwords = set(stopwords)

        self.rt_rex = re.compile(r'^RT\s*:\s*')
        self.space_rex = re.compile(r'\s+')
        self.stop_rex = re.compile(r'\s?\W(\#rt|XD|rt)\W?\s?', re.IGNORECASE)

    def extract(self, json):
        if 'entities' not in json or \
           'hashtags' not in json['entities']:

            hts = Extractor(json['text']).extract_hashtags_with_indices()
        else:
            hts = json['entities']['hashtags']

        return filter(lambda x: x not in self.stopwords,
                      map(lambda x: x['text'], hts))

    def sanitize(self, tweet_text):
        extractor = Extractor(tweet_text)

        for meth in (extractor.extract_urls,
                     lambda: extractor.extract_mentioned_screen_names(lambda x: '@' + x)):
            iterable = meth()

            if not iterable:
                continue

            for pattern in iterable:
                tweet_text = tweet_text.replace(pattern, '')

        tweet_text = self.rt_rex.sub('', tweet_text)
        tweet_text = self.stop_rex.sub(' ', tweet_text)
        tweet_text = self.space_rex.sub(' ', tweet_text)
        return tweet_text.strip()#.replace('\n', '').replace('  ', ' ').replace('\t', '').replace('\r', '')

if __name__ == "__main__":
    he = HashtagExtractor()
    print he.sanitize(u"RT @AntonioCinotti: Esempio di un personaggio che si è creato una nuova opportunità dal nulla, @LuigiCentenaro #schoolsnif #snif #snid h ...")
    print he.sanitize(u"@ShyCarotaHidds Io devo andare,a dopo :3 Ci sei stasera? In caso scrivimi che ti rispondo dopo ;) #Bye")
    print he.sanitize(u"#SwitzerlandWantsA1DSigningToo aiutiamole anche loro hanno bisogno di vederli un #RT non costa nulla")
    print he.sanitize(u"this a test XD")
    print he.sanitize(u"another testhttp://www.google.com")

