#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

prevtemplate = None
template = ''
pages = []

for line in sys.stdin:
    template, page, id = line.strip().split('\t', 2)
    id = int(id)

    if template == prevtemplate:
        pages.append((id, page))
    else:
        if prevtemplate:
            pages.sort()
            print >> sys.stdout, json.dumps({
                'template': prevtemplate,
                'pages': map(lambda x: x[1], pages),
                'ids': map(lambda x: x[0], pages),
            }, sort_keys=True)

        prevtemplate = template
        pages = [(id, page)]

if prevtemplate == template:
    pages.sort()
    print >> sys.stdout, json.dumps({
        'template': prevtemplate,
        'pages': map(lambda x: x[1], pages),
        'ids': map(lambda x: x[0], pages),
    }, sort_keys=True)