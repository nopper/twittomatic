import sys
import json

for line in sys.stdin:
    src = json.loads(line)
    src_id = src['id']
    for dst in src['links']:
        print "%d\t%d" % (src_id, dst['id'])
