import sys
import json
import plyvel
import struct

counter = 0
db = plyvel.DB('wikipedia-portals', create_if_missing=True)

for line in sys.stdin:
    obj = json.loads(line)

    portals = []
    page_id = obj['id']

    for templ in obj['templates']:
        if templ['name'].lower().startswith('portale:'):
            portals.append(templ['name'].lower().replace('portale:', '').replace('/icona', ''))

    db.put(struct.pack("!I", page_id), json.dumps(sorted(portals)))
    counter += 1

db.close()

print "%d total portals extracted" % counter
