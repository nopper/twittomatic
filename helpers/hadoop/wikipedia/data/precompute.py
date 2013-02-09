"""
Script to recompute the relatedness matrix
"""

import plyvel

from math import log
from array import array
from struct import pack, unpack

indb = plyvel.DB('wikipedia-indeg', lru_cache_size=1024*1024*100)
outdb = plyvel.DB('wikipedia-outdeg', lru_cache_size=1024*1024*100)
scoredb = plyvel.DB('relatedness-score', create_if_missing=True)

def get_candidates(source, page_ids):
    candidates = set()

    for page_id in page_ids:
        for candidate in array("I", outdb.get(pack("!I", page_id))).tolist():
            if candidate > source:
                candidates.add(candidate)

    return sorted(candidates)

LOGW = log(1476372 + 368267)

for key, value in indb:
    a_id = unpack("!I", key)[0]
    a_in_ids = set(array("I", value).tolist())

    for count, b_id in enumerate(get_candidates(a_id, a_in_ids)):
        b_in_ids = set(array("I", indb.get(pack("!I", b_id))).tolist())

        A = len(a_in_ids)
        B = len(b_in_ids)
        AB = len(a_in_ids.intersection(b_in_ids))
        relatedness = (log(max(A, B)) - log(AB)) / (LOGW - log(min(A, B)))

        if relatedness > 0:
            scoredb.put(pack("!II", a_id, b_id), pack("!f", relatedness))

        if count % 1000 == 0:
            print "REL", a_id, b_id, relatedness

