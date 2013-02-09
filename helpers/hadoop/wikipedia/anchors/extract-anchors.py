import sys
import json

for line in sys.stdin:
    obj = json.loads(line.rstrip('\n'))
    anchor = obj['anchor'].strip()
    for page in obj['pages']:
        page_title = page[0].strip().lower().replace(' ', '_')
        page_count = page[1]

        print anchor, page_count, page_title
