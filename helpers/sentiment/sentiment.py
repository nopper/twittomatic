import os

from splitter import *
from mwnet import MWNet
from sentiwordnet import SentiWordNetCorpusReader

class Analyzer(object):
    def __init__(self):
        self.mwnet = MWNet(os.path.join("data", "mwnet.db"))
        self.swn = SentiWordNetCorpusReader(os.path.join("data", "SentiWordNet_3.0.0.txt"))

        if os.getenv('TANL_EMAIL'):
            self.splitter = TanlSplitter()
        else:
            self.splitter = SimpleSplitter()

    def analyze_sentence(self, sentence):
        scores = []
        result = {}

        for word, lemma, tag, wn_type, indices in self.splitter.iter_words(sentence):
            # Here we can also impose the type to be an ADJ, NAME, or VERB
            synsets = self.mwnet.get_english_synsets(lemma, wn_type)

            if not synsets:
                continue

            synsets_dict = {}

            found = False
            for syn in synsets:
                for translation in self.mwnet.get_translation(syn):
                    senti_synsets = self.swn.senti_synsets(translation, wn_type)

                    if not senti_synsets:
                        continue

                    synsets_dict[syn] = map(lambda x: [x.synset.name, x.pos_score, x.neg_score, x.obj_score], senti_synsets)
                    scores.extend(map(lambda x: (x.pos_score, x.neg_score, x.obj_score), senti_synsets))

                if found:
                    break

            positive  = map(lambda x: x[0], scores)
            negative  = map(lambda x: x[1], scores)
            objective = map(lambda x: x[2], scores)

            if len(positive) > 0:
                pscore = sum(positive) * 1.0 / len(positive)
                nscore = sum(negative) * 1.0 / len(positive)
                oscore = sum(objective) * 1.0 / len(positive)

                result[word] = {
                    'indices': indices,
                    'lemma': lemma,
                    'features': tag,
                    'synsets': synsets_dict,
                    'scores': {
                        'positive': pscore,
                        'negative': nscore,
                        'objective': oscore,
                    },
                }

        return result

if __name__ == "__main__":
    import sys
    app = Analyzer()
    print app.analyze_sentence(sys.argv[1])

    #text = "Il presidente Giorgio Napolitano incontra a Roma il commissario dell'Unione Europea Manuel Barroso."

    #for i in iter_words(text):
    #    print i
