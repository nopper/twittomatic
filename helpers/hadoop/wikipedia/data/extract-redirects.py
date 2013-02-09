import sys
import plyvel

db = plyvel.DB('wikipedia-titles')

counter = 0
skipped = 0

for line in sys.stdin:
    args = line.rstrip('\n').split(' ')

    try:
        dbpedia_source = args[0].rsplit('dbpedia.org/resource/', 1)[-1][:-1].lower().replace('_', ' ')
        dbpedia_dest = args[2].rsplit('dbpedia.org/resource/', 1)[-1][:-1].lower()
        db.put("title:%s" % dbpedia_source, db.get("title:%s" % dbpedia_dest))
        counter += 1
    except Exception, exc:
        skipped += 1

db.close()
print >> sys.stderr, "%d redirects extracted, %d not present" % (counter, skipped)
