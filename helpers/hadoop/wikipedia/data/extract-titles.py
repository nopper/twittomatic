import sys
import json
import plyvel
from struct import pack

redirects = (len(sys.argv) > 1)

db = plyvel.DB('wikipedia-titles', create_if_missing=(not redirects))
counter = 0

for line in sys.stdin:
    obj = json.loads(line.rstrip('\n'))

    page_id = obj['id']
    title = obj['name'].replace(" ", "_").lower().encode('utf8')

    db.put("title:%s" % title, str(page_id))
    db.put("id:%s" % pack("!I", page_id), title)
    db.put("length:%s" % pack("!I", page_id), pack("!I", obj['length']))
    counter += 1

db.close()
print "%d Wikipedia titles extracted" % (counter)
