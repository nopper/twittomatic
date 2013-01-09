import sqlite3

class MWNet(object):
    def __init__(self, mwnetfile="mwnet.db"):
        self.connection = sqlite3.connect(mwnetfile, check_same_thread=False)

    def get_english_synsets(self, lemma, wn_type=None):
        cursor = self.connection.cursor()
        cursor.execute("SELECT id_n, id_v, id_a, id_r FROM italian_index WHERE lemma = ?", (lemma, ))
        result = cursor.fetchone()

        if not result:
            return []

        print result, wn_type

        if wn_type == 'n':
            result = [result[0]]
        if wn_type == 'v':
            result = [result[1]]
        if wn_type == 'a':
            result = [result[2]]
        if wn_type == 'r':
            result = [result[3]]

        synsets = []
        for id_list in result:
            if not id_list:
                continue
            synsets.extend(id_list.split(' '))

        return synsets

    def get_translation(self, synset):
        cursor = self.connection.cursor()
        cursor.execute("SELECT word FROM english_synset WHERE id = ?", (synset, ))
        result = cursor.fetchone()

        if not result:
            return []

        trans = []
        for words in result:
            for word in words.split(' '):
                word = word.replace('_', ' ')
                if word:
                    trans.append(word)

        return trans

if __name__ == "__main__":
    import sys
    app = MWNet()
    synsets = app.get_english_synsets(sys.argv[1])
    for syn in synsets:
        print syn, app.get_translation(syn)
