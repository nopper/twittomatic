import os
import dawg

class LanguageChecker(object):
    def __init__(self, lang):
        self.dawg = dawg.DAWG()
        self.dawg.load(os.path.join(os.path.dirname(__file__), lang + '.dawg'))

    def is_valid(self, phrase):
        matched, total = 0, 0

        for word in phrase.lower().split('\t')[-1].split():
            if word in self.dawg:
                matched += 1
            total += 1

        if (total <= 4 and matched >= 3) or \
           (total > 4 and matched > (total / 2.0)):
            return True

        return False
