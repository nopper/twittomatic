from mwnet import MWNet
from morphit import MorphIt
from sentiwordnet import SentiWordNetCorpusReader

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

class Analyzer(object):
    def __init__(self):
        self.mwnet = MWNet()
        self.morpher = MorphIt()
        self.swn = SentiWordNetCorpusReader("SentiWordNet_3.0.0.txt")

        # TODO: complete this list. Also take in consideration that morphit provides
        # tagging for proper names, smilies and so on which can be discarded quite
        # easily

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

    def analyze_sentence(self, sentence):
        scores = []
        result = {}

        for word, indices in iter_words(sentence):
            ret = self.morpher.find(word)

            if not ret:
                continue

            word, lemma, features = ret
            wn_type = self.get_type(features)

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

            print scores

            positive  = map(lambda x: x[0], scores)
            negative  = map(lambda x: x[1], scores)
            objective = map(lambda x: x[2], scores)

            result[word] = {
                'indices': indices,
                'lemma': lemma,
                'features': features,
                'synsets': synsets_dict,
                'scores': {
                    'positive': sum(positive) * 1.0 / len(positive),
                    'negative': sum(negative) * 1.0 / len(positive),
                    'objective': sum(objective) * 1.0 / len(positive),
                },
            }

        print scores
        return result

if __name__ == "__main__":
    import sys
    app = Analyzer()
    print app.analyze_sentence(sys.argv[1])
