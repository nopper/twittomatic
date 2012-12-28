#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

from lxml import etree

for line in sys.stdin:
    try:
        page = json.loads(line.strip())
        root = etree.XML('<doc>%s</doc>' % page['html'])

        for anchor in root.findall('.//a[@href]'):

            if not anchor.getchildren() and \
               'title' in anchor.keys() and \
               anchor.get('href').startswith('/wiki/'):

                print >> sys.stdout, "%s\t%s" % (anchor.text, anchor.get('title'))

    except Exception, exc:
        print >> sys.stderr, "Error during the processing of line", line
        pass
