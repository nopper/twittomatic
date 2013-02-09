import os
import sys
from utils import *

import plyvel

class Precomputer(object):
    def __init__(self, options):
        self.options = options
        #self.extract_mappings()
        self.extract_anchors()
        #self.extract_templates()

        self.list_anchors()

    def list_anchors(self):
        with open('anchors.txt', 'w') as outputfile:
            db = plyvel.DB('wiki-anchors')

            for key, value in db:
                outputfile.write(key + '\n')

    def extract_templates(self):
        with profiled("Wikipedia mappings loaded in %s"):
            db = plyvel.DB('wiki-templates', create_if_missing=True)

            for page_id, templates in iterate_templates(self.options.templates):
                portal_list = []
                template_list = []

                for tmp_id, tmp_name in sorted(zip(
                    map(lambda x: x['id'], templates),
                    map(lambda x: x['name'], templates))):

                     if tmp_name.lower().startswith("portale:"):
                        portal_list.append(tmp_id)
                     else:
                        template_list.append(tmp_id)

                portal_list.sort()
                template_list.sort()

                db.put('id:%s' % page_id, json.dumps({'templates': template_list, 'portals': portal_list}))

    def extract_anchors(self):
        with profiled("Anchors loaded in %s"):
            skipped = 0
            titles = plyvel.DB('wiki-mappings')
            db = plyvel.DB('wiki-anchors', create_if_missing=True)

            for count, (label, pages) in enumerate(iterate_anchors(self.options.anchors)):
                output = []

                for page_name, page_score in pages:
                    title = page_name.lower().strip().encode('utf8')
                    page_id = titles.get('title:%s' % title)

                    if page_id is None:
                        skipped += 1
                        continue


                    output.append((int(page_score), int(page_id)))

                try:
                    label = label.lower()
                    label = label.encode('utf8')
                except:
                    pass

                if output:
                    # Sort by score
                    output = map(lambda x: {"score": x[0], "id": x[1]}, sorted(output, reverse=True))
                    db.put('anchor:%s' % label, json.dumps(output, sort_keys=True))

            print "%d anchors of which %d skipped" % (count, skipped)

    def extract_mappings(self):
        with profiled("Wikipedia mappings loaded in %s"):
            db = plyvel.DB('wiki-mappings', create_if_missing=True)

            for page_id, page_name, page_title, page_length in iterate_mappings(options.mappings):
                title = page_title.lower().encode('utf8')
                db.put('title:%s' % title, str(page_id))
                db.put('id:%s' % page_id, json.dumps({'name': page_name, 'length': page_length}))

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser("%s [options]" % sys.argv[0])
    parser.add_option("-m", "--mappings", dest="mappings", default="wiki-titles.json.gz",
                      help="File containing the Wikipedia page mappings (default: wiki-titles.json.gz)")
    parser.add_option("-t", "--templates", dest="templates", default="wiki-templates.json.gz",
                      help="File containing the Wikipedia template informations (default: wiki-templates.json.gz)")
    parser.add_option("-a", "--anchors", dest="anchors", default="wiki-anchors.json.gz",
                      help="File containing the extracted Wikipedia anchors (default: wiki-anchors.json.gz)")

    (options, args) = parser.parse_args()

    def check_file(filename):
        if not os.path.exists(filename):
            print "File %s does not exists" % filename
            parser.print_help()
            sys.exit(-1)

    map(check_file, (options.mappings, options.templates, options.anchors))

    app = Precomputer(options)
