"""
Simple script that takes in input wikipedia HTML and produces plain text
"""
import sys
from json import loads
from StringIO import StringIO
from lxml import html
from lxml.html.clean import clean_html

for line in sys.stdin:
    obj = loads(line)
    tree = html.fromstring(obj['html'].encode('utf16'))
    tree = clean_html(tree)
    print tree.text_content().encode('utf8').lower().replace("[modifica]", "")
    #for word in tree.text_content().split():
    #    print word.encode('utf8').lower()
