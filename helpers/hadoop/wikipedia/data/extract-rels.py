import sys
import plyvel
from json import loads
from array import array
from struct import pack, unpack
from collections import defaultdict

indb = plyvel.DB('wikipedia-indeg', create_if_missing=True)
outdb = plyvel.DB('wikipedia-outdeg', create_if_missing=True)

counter = 0
inpages = defaultdict(set)
outpages = defaultdict(list)

for line in sys.stdin:
    obj = loads(line)

    src = obj['id']

    for outpage in obj['links']:
        dst = outpage['id']

        inpages[dst].add(src)
        outpages[src].append(dst)
        counter += 1

print "%d relations extracted" % counter

print "Saving in- relations"

for page, ids in sorted(inpages.items()):
    page_id = pack("!I", page)
    sorted_ids = array("I", sorted(ids))

    indb.put(page_id, sorted_ids.tostring())

print "Saving out- relations"

for page, ids in sorted(outpages.items()):
    page_id = pack("!I", page)
    sorted_ids = array("I", sorted(set(ids)))

    outdb.put(page_id, sorted_ids.tostring())
