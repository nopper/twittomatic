"""
Simple script that ouputs a GraphML file
"""

import gzip
from xml.sax.saxutils import XMLGenerator
from xml.sax.xmlreader import AttributesNSImpl

class XMLWriter(object):
    def __init__(self, output):
        self.output = XMLGenerator(output, "utf-8")
        self.level = 0

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

    def start_document(self, keys):
        self.start_element('graphml', {
            "xmlns": u"http://graphml.graphdrawing.org/xmlns",
            "xmlns:xsi": u"http://www.w3.org/2001/XMLSchema-instance",
            "xsi:schemaLocation": u"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd",
        })

        for kid, kfor, kname, ktype in keys:
            self.start_element('key', {
                "id": kname,
                "for": kfor,
                "attr.name": kname,
                "attr.type": ktype,
            })
            self.end_element('key')

        self.start_element('graph', {
            'id': 'G',
            'edgedefault': 'undirected',
        })

    def end_document(self):
        self.end_element('graph')
        self.output.endElementNS((None, u'graphml'), u'graphml')
        self.output.endDocument()

    def attrs(self, attributes):
        return AttributesNSImpl(dict(((None, k), v) for (k,v) in attributes.iteritems()), {})

class GraphMLRenderer(object):
    def __init__(self, options):
        self.inputfile = options.inputfile
        self.outputfile = options.outputfile
        self.ht_nodes = options.ht_nodes
        self.wiki_nodes = options.wiki_nodes

    def run(self):
        with gzip.open(self.outputfile, 'w') as output:
            xml = XMLWriter(output)
            xml.start_document([
                ('weight', 'edge', 'weight', 'string'),
                ('name', 'node', 'name', 'string'),
                ('title', 'node', 'title', 'string'),
            ])


            hashtags = self.add_nodes(xml, self.ht_nodes, 'name')
            pages = self.add_nodes(xml, self.wiki_nodes, 'title')

            edges = self.add_edges(xml)

            xml.end_document()

            print "%d hashtags, %d wikipedia pages, %d edges" % (hashtags, pages, edges)

    def add_edges(self, xml, lastid=200000000):
        with gzip.open(self.inputfile, 'r') as inputfile:
            for count, line in enumerate(inputfile):
                try:
                    ht_id, wiki_id, rhos, ht_name, wiki_name = line.strip().split('\t', 4)
                except:
                    ht_id, wiki_id, rhos, ht_name = line.strip().split('\t', 3)
                    wiki_name = ''

                xml.start_element('edge', {
                    "id": str(lastid),
                    "source": ht_id,
                    "target": wiki_id,
                    "label": "linked",
                })
                xml.start_element('data', {'key': 'weight'}, True)
                xml.output.characters(rhos)
                xml.end_element('data', True)
                xml.end_element('edge')

                lastid += 1

            return count + 1

    def add_nodes(self, xml, filename, attribute):
        with open(filename, 'r') as inputfile:
            for count, line in enumerate(inputfile):
                try:
                    node_id, node_name = line.strip().split('\t', 1)
                except:
                    node_id = line.strip()
                    node_name = ''

                xml.start_element('node', {'id': node_id})
                xml.start_element('data', {'key': attribute}, True)
                xml.output.characters(node_name)
                xml.end_element('data', True)
                xml.end_element('node')

            return count + 1


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser(description="Render the HE graph in GraphML format")
    parser.add_option("-i", "--input", dest="inputfile",
                      help="Graph file in tsv.gz format")
    parser.add_option("-o", "--output", dest="outputfile",
                      help="Output file")
    parser.add_option("--ht-nodes", dest="ht_nodes",
                      help="Hashtag nodes")
    parser.add_option("--wiki-nodes", dest="wiki_nodes",
                      help="Wikipedia nodes")

    (options, args) = parser.parse_args()

    if options.inputfile and options.outputfile:
        app = GraphMLRenderer(options)
        app.run()
    else:
        parser.print_help()