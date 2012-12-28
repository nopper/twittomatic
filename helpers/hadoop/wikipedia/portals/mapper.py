#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

for line in sys.stdin:
    try:
        page = json.loads(line.strip())
        for template in page['templates']:
            if template['name'].startswith('Portale:'):
                print >> sys.stdout, "%s\t%s\t%d" % (template['name'], page['name'], page['id'])
    except Exception, exc:
        print >> sys.stderr, "Error during the processing of line", line
        raise exc
