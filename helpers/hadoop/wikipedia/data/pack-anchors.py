import sys
import json
import plyvel
from collections import defaultdict

db = plyvel.DB('wikipedia-anchors', create_if_missing=True)

counter = 0
anchors = defaultdict(lambda: defaultdict(int))

for line in sys.stdin:
    anchor, link_count, page_id = line.rstrip('\n').rsplit('\t', 2)
    anchors[anchor][int(page_id)] += int(link_count)

for anchor, pages in sorted(anchors.items()):
    db.put(anchor, json.dumps([(pid, lp) for (pid, lp) in sorted(pages.items())]))
    counter += 1

db.close()

print "%d total anchors extracted" % counter
