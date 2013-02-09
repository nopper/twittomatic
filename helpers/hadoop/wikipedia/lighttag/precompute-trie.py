import codecs
import marisa_trie

keys = []
with codecs.open('anchors-sorted.txt', 'r', 'utf8') as input:
    for line in input:
        keys.append(line[7:].strip())

trie = marisa_trie.Trie(keys)
trie.save('anchors.marisa')
