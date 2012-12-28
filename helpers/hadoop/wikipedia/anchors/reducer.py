#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

from collections import defaultdict

prevanchor = None
titles = defaultdict(int)

for line in sys.stdin:
    anchor, page = line.strip().split('\t', 1)

    if anchor == prevanchor:
        titles[page] += 1
    else:
        if prevanchor:
            print >> sys.stdout, json.dumps({
                'anchor': prevanchor,
                'pages': [(k, v) for k,v in sorted(titles.items(), key=lambda x: x[1])],
            }, sort_keys=True)

        prevanchor = anchor
        titles = defaultdict(int)
        titles[anchor] += 1

if prevanchor == anchor:
    print >> sys.stdout, json.dumps({
        'anchor': prevanchor,
        'pages': [(k, v) for k,v in sorted(titles.items(), key=lambda x: x[1])],
    }, sort_keys=True)
