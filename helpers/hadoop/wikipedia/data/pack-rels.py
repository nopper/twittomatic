import sys
import plyvel
from struct import pack
from array import array

db = plyvel.DB('wikipedia-' + sys.argv[1], create_if_missing=True)

iterable = iter(sys.stdin)
prevsrc, dst = map(int, iterable.next().rstrip('\n').split('\t', 1))
adj = [dst]

counter = 0

for line in iterable:
    src, dst = map(int, line.rstrip('\n').split('\t', 1))

    if src == prevsrc:
        adj.append(dst)
    else:
        page_id = pack("!I", prevsrc)
        sorted_ids = array("I", adj)
        db.put(page_id, sorted_ids.tostring())
        counter += 1

        prevsrc = src
        adj = [dst]

if adj:
    page_id = pack("!I", src)
    sorted_ids = array("I", adj)
    db.put(page_id, sorted_ids.tostring())
    counter += 1

db.close()

print "%d relations extracted" % counter
