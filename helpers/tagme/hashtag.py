# encoding=utf8
import re
from twitter_text import Extractor

class HashtagExtractor(object):
    def __init__(self, stopwords=None):
        if not stopwords:
            self.stopwords = set(['rt', 'RT', 'Rt'])
        else:
            self.stopwords = set(stopwords)

        self.rt_rex = re.compile(r'^RT\s*(:\s*)*')
        self.space_rex = re.compile(r'\s+')
        self.stop_rex = re.compile(r'\s?\W(\#rt|XD|rt)\W?\s?', re.IGNORECASE)

    def extract(self, json):
        "Extract hashtags (lower case) and return an array containing them in lexicographic order"
        if 'entities' not in json or \
           'hashtags' not in json['entities']:

            hts = Extractor(json['text']).extract_hashtags_with_indices()
        else:
            hts = json['entities']['hashtags']

        ret = filter(lambda x: x not in self.stopwords,
                     set(map(lambda x: x['text'].lower(), hts)))
        ret.sort()
        return ret

    def sanitize(self, tweet_text):
        retweet = (tweet_text.startswith('RT') and len(tweet_text) >= 135)
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

        try:
            # Try to avoid trimmed text due to the nature of retweet that may not fit

            if retweet:
                sentence, trimmed, terminator = tweet_text.rsplit(' ', 2)

                if terminator == '...' or terminator == u'…':
                    return sentence
        except:
            pass

        return tweet_text.strip()#.replace('\n', '').replace('  ', ' ').replace('\t', '').replace('\r', '')

if __name__ == "__main__":
    he = HashtagExtractor()
    print he.sanitize(u"RT: @AntonioCinotti: Esempio di un personaggio che si è creato una nuova opportunità dal nulla, @LuigiCentenaro #schoolsnif #snif #snid h ...")
    print he.sanitize(u"RT @AntonioCinotti: Esempio di un personaggio che si è creato una nuova opportunità dal nulla, @LuigiCentenaro #schoolsnif #snif #snid h ...")
    print he.sanitize(u"@ShyCarotaHidds Io devo andare,a dopo :3 Ci sei stasera? In caso scrivimi che ti rispondo dopo ;) #Bye")
    print he.sanitize(u"#SwitzerlandWantsA1DSigningToo aiutiamole anche loro hanno bisogno di vederli un #RT non costa nulla")
    print he.sanitize(u"this a test XD")
    print he.sanitize(u"another testhttp://www.google.com")
    print he.sanitize(u"#GDTHATXX mi piace :D XD però preferivo one of a kind... non uccidetemi xD")
    print he.sanitize(u"ciao come asdXDio")