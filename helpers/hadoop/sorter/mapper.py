#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

for line in sys.stdin:
    try:
        tweet = json.loads(line.strip())
        output = "%s.%s" % (tweet['id_str'], line.strip())
        print >> sys.stdout, output
    except Exception, exc:
        print >> sys.stderr, "Error during the processing of line", line
        raise exc
