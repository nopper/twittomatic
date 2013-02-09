#!/usr/bin/env python
# encoding=utf8

import os
import sys
from utils import profiled
from itertools import izip_longest
from disambiguate import Disambiguator

import plyvel
import marisa_trie


def extract_words(text):
    words = []
    startpos = 0
    text = text.lower()

    for pos, ch in enumerate(text.lower()):
        if ch == ' ' or ch == '\t' or ch == '\n' or ch == '\r':
            if pos != startpos:
                words.append((text[startpos:pos], (startpos, pos)))
            startpos = pos + 1

    if pos != startpos:
        words.append((text[startpos:pos + 1], (startpos, pos + 1)))

    print words

    return words


class LightTag(object):
    def __init__(self, filetrie="anchors.marisa", filestop="stop.txt"):
        self.trie = marisa_trie.Trie()
        self.disambig = Disambiguator()
        self.stopwords = set()

        with open(filestop, 'r') as stopfile:
            self.stopwords = set(filter(lambda x: x and x[0] != '#', map(lambda x: x.rstrip(), stopfile.readlines())))

        with open(filetrie, 'r') as inputfile:
            self.trie.read(inputfile)

        print "Loaded %d anchors" % len(self.trie)

    def annotate(self, text):
        with profiled("Disambiguated in %s"):
            matches = self.match(text)

            spots = []
            indices = {}
            for (start, stop), anchors in matches.items():
                if anchors[0] in self.stopwords:
                    print "Ignoring stopword", anchors[0], anchors
                    continue

                spots.append(anchors[0])
                indices[anchors[0]] = (start, stop)

            ret = {}
            results = self.disambig.disambiguate(spots)

            for spot in results:
                ret[spot] = results[spot]
                ret[spot]['indices'] = indices[spot]
                start, stop = indices[spot]
                ret[spot]['spot'] = text[start:stop]

            return [v for(k, v) in ret.items()]

    def match(self, text, context=5, threshold=0.8):
        anchors = {}

        with profiled("Annotated in %s"):
            start = 0
            text_words = extract_words(text)

            while start < len(text_words):
                stop = min(start + context, len(text_words)) 

                while stop >= start:
                    words = map(lambda x: x[0], text_words[start:stop])
                    target = u' '.join(words)

                    # Only consider strings which are at least 3 characters long
                    if len(target) >= 3:
                        begin = text_words[start][1][0]
                        end   = text_words[stop - 1][1][1]

                        #assert target == text[begin:end].lower()

                        wiki_titles = self.trie.keys(target)
                        wiki_titles = [title for title in self.filter_wiki_titles(words, wiki_titles, threshold=threshold)]

                        if wiki_titles:
                            anchors[(begin, end)] = wiki_titles
                            stop = 0

                    stop -= 1
                start += 1

        return anchors

    def filter_wiki_titles(self, words, wiki_titles, threshold=0.8):
        """
        Filter a list of wiki titles based on similarity
        """

        #print "Filtering", words, wiki_titles
        for title in wiki_titles:
            similarities = 0

            for counter, (word1, word2) in enumerate(izip_longest(words, title.split())):
                if not word1 or not word2:
                    continue

                if word1 == word2:
                    similarities += 1

            sim = similarities / float(counter + 1)

            if sim > threshold:
                yield title

                if sim == 1.0:
                    raise StopIteration

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser("%s [options]" % sys.argv[0])
    parser.add_option("-t", "--trie", default="anchors.marisa",
                      help="Marisa trie file containing anchors information")
    (options, args) = parser.parse_args()

    app = LightTag(options.trie)
    app.annotate(u"Uomini e donne è un programma televisivo di merda")
    app.annotate(u"Il 10 novembre 1938 Fermi venne insignito del premio nobel per i suoi studi nel settore della fisica nucleare")
    app.annotate(u"Il settore del mio disco fisso e' andato. Ho perso tutti i miei fottuti dati. Dovevo fare un backup cazzo!")
    app.annotate(u"Certo che il berlusconi sembra proprio un nano da giardino. Dopo il governo monti penso che andremo giu di bound")
    app.annotate(u"Non vedo l'ora di vedere l'ultimo film di quentin tarantino")
