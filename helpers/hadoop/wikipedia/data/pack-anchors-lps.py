import sys
import json
import plyvel

db = plyvel.DB('wikipedia-anchors')

for line in sys.stdin:
    anchor, lp = line.rstrip('\n').rsplit('\t', 1)
    value = db.get(anchor)

    if value:
        value = json.loads(value)
        value.insert(0, int(lp))
        db.put(anchor, json.dumps(value))

db.close()
