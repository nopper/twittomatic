"""
Simple script to extract social graph from the dataset
"""

# TODO: we can add the possibility to take in consideration only part of the graph

import os
import sys
import gzip
import json
import struct

from collections import defaultdict
from xml.sax.saxutils import XMLGenerator
from xml.sax.xmlreader import AttributesNSImpl

class SocialGraphExport(object):
    def __init__(self, userinfo, dataset, output):
        self.gzfd = gzip.open(output, "w")
        self.output = XMLGenerator(self.gzfd, "utf-8")
        self.level = 0

        self.start_document()
        self.userinfo = self.load_userinfo(userinfo)

        for user_id in self.userinfo:
            self.start_element('node', {'id': str(user_id)}, False)
            self.start_element('data', {'key': 'screen_name'}, True)
            self.output.characters(self.get_screen_name(user_id))
            self.end_element('data', True)
            self.end_element('node', False)

        self.dataset = os.path.abspath(dataset)
        self.extract_graph()
        self.end_document()

    def load_userinfo(self, inputfile):
        info = {}
        with gzip.open(inputfile, 'r') as input:
            for line in input:
                user = json.loads(line.strip())
                info[int(user['id_str'])] = user
        return info

    def get_screen_name(self, user_id):
        return self.userinfo[user_id]['screen_name']

    def attrs(self, attributes):
        return AttributesNSImpl(dict(((None, k), v) for (k,v) in attributes.iteritems()), {})

    def start_element(self, name, attrs, nochar=False):
        self.output.characters(' ' * self.level)
        self.output.startElementNS((None, name), name, self.attrs(attrs))

        if not nochar:
            self.output.characters('\n')

        self.level += 1

    def end_element(self, name, nochar=False):
        self.level -= 1

        if not nochar:
            self.output.characters(' ' * self.level)

        self.output.endElementNS((None, name), name)
        self.output.characters('\n')

    def start_document(self):
        self.start_element('graphml', {
            "xmlns": u"http://graphml.graphdrawing.org/xmlns",
            "xmlns:xsi": u"http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": u"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd",
        })

        self.start_element('key', {
            "id": "screen_name",
            "for": "node",
            "attr.name": "screen_name",
            "attr.type": "string",
        })
        self.end_element('key')

        self.start_element('graph', {
            'id': 'G',
            'edgedefault': 'directed',
        })

    def end_document(self):
        self.end_element('graph')
        self.output.endElementNS((None, u'graphml'), u'graphml')
        self.output.endDocument()
        self.gzfd.close()

    def extract_graph(self):
        lastid = 1
        lastuser = 0
        users = set(self.userinfo.keys())

        for user_id in users:
            opener = open
            filename = os.path.join(self.dataset, str(user_id)[:2], str(user_id) + '.fws')

            if not os.path.exists(filename):
                filename += '.gz'
                opener = gzip.open

                if not os.path.exists(filename):
                    continue

            followers = set()
            datasize = struct.calcsize('!Q')

            with opener(filename, 'r') as ffile:
                while True:
                    data = ffile.read(datasize)
                    if not data:
                        break

                    following = struct.unpack('!Q', data)[0]
                    followers.add(following)

                for following in followers.intersection(users):
                    self.start_element('edge', {
                        "id": str(lastid),
                        "source": str(user_id),
                        "target": str(following),
                        "label": "follows",
                    }, True)
                    self.end_element('edge', True)
                    lastid += 1

                lastuser += 1
                sys.stderr.write('\rUser: %d of %d [%02d%%]' % (lastuser, len(users), lastuser * 100.0 / len(users)))
                sys.stderr.flush()

        sys.stderr.write('\nCompleted')
        sys.stderr.flush()

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(description="Create a social graph from the dataset")
    parser.add_option("-d", "--dataset", dest="dataset",
                      help="Dataset where the .twt or .twt.gz files reside")
    parser.add_option("-u", "--userinfo", dest="userinfo",
                      help="User info file (.json.gz)")
    parser.add_option("-o", "--output", dest="output", default="friends.graphml.gz",
                      help="Output file (default: friends.graphml.gz)")

    (options, args) = parser.parse_args()

    if options.dataset and options.userinfo and options.output:
        SocialGraphExport(options.userinfo, options.dataset, options.output)
    else:
        parser.print_help()
