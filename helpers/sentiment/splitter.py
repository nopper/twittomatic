import os
import requests
from morphit import MorphIt
from StringIO import StringIO

class Splitter(object):
    def iter_words(self, sentence):
        pass

def strip_word(text, start, stop):
    while start < stop:
        if text[start] in "'\":;,.":
            start += 1
        break

    while stop > start:
        if text[stop] in "'\":,.":
            stop -= 1
        break

    if stop == len(text) - 1:
        stop += 1

    return text[start:stop], (start, stop)

def iter_words(text):
    prevpos = 0
    for pos, ch in enumerate(text):
        if ch in ' \n\r\t':
            yield strip_word(text, prevpos, pos)
            prevpos = pos + 1

    yield strip_word(text, prevpos, pos)

class SimpleSplitter(Splitter):
    def __init__(self):
        self.morpher = MorphIt(os.path.join("data", "morph-it.txt"))
        self.morph_to_wn = {
            "VER" : 'v',
            "DET-INDEF": 'a',
            "NOUN-M": 'n',
            "NOUN-F": 'n',
        }

    def get_type(self, features):
        try:
            return self.morph_to_wn[features.split(':')[0]]
        except:
            return None

    def iter_words(self, sentence):
        for word, indices in iter_words(sentence):
            ret = self.morpher.find(word)

            if ret:
                word, lemma, features = ret
                wn_type = self.get_type(features)
            else:
                word, lemma, features = word, word, 'n'
                wn_type = 'n'

            yield word, lemma, features, wn_type, indices

class TanlSplitter(Splitter):
    def __init__(self):
        # Require an access token from http://tanl.di.unipi.it/it/api
        self.tanl_conf = {
            'email': os.getenv('TANL_EMAIL'),
            'token': os.getenv('TANL_TOKEN'),
            'authentication': os.getenv('TANL_AUTHENTICATION'),
            'service': 'tokenize'
        }

        print "Using TANL", self.tanl_conf.items()

        self.tanl_to_wn = {
            "V": 'v',
            "A": 'a',
            "S": 'n',
            "B": 'r',
        }

    def iter_words(self, text):
        """
        Exploit TANL both for sentence and word tokenizer and for POS tagging.
        """
        data = self.tanl_conf

        fileobj = StringIO(text)
        r = requests.post(url='http://tanl.di.unipi.it/it/api', data=data, files = {'file': fileobj})
        body = r.text
        prevstop = 0

        if r.status_code != 200:
            print "Error while querying TANL (RET code %d)" % r.status_code
            raise StopIteration

        for line in body.splitlines():
            # Return the root form
            args = line.split('\t')

            if len(args) != 4:
                raise StopIteration

            word = args[0]
            root = args[1]
            tag  = args[2]

            start = text.index(word, prevstop)
            stop = start + len(word)

            prevstop = stop

            assert text[start:stop] == word

            wn_type = self.tanl_to_wn.get(tag[0].upper(), None)

            if not wn_type:
                continue

            yield word, root, tag, wn_type, (start, stop)
