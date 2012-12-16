#!/usr/bin/env python

import sys
import json
import time
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

for line in sys.stdin:
    id, line = line.strip().split('.', 1)
    print line
