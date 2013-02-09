import sys
import json
import plyvel

db = plyvel.DB('wikipedia-titles')

counter = 0
skipped = 0

for line in sys.stdin:
    obj = json.loads(line.rstrip('\n'))
    anchor = obj['anchor'].strip().lower()

    for page in obj['pages']:
        page_title = page[0].strip().lower().replace(' ', '_').encode('utf8')
        page_count = page[1]

        page_id = db.get('title:%s' % page_title)

        if page_id is not None:
            print "%s\t%s\t%s" % (anchor.encode("utf8"), page_count, page_id)
            counter += 1
        else:
            skipped += 1
            #print >> sys.stderr, "title", page_title, "not found"

db.close()

print >> sys.stderr, "%d anchors extracted, %d skipped" % (counter, skipped)
