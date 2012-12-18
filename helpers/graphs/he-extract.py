import sys
import gzip
import json
import heapq

from bisect import bisect
from tempfile import TemporaryFile

from xml.sax.saxutils import XMLGenerator
from xml.sax.xmlreader import AttributesNSImpl

START_ID = 100000000
EDGE_START_ID = 200000000

class Entry(object):
    def __init__(self, ht, annotation, rho, title):
        self.ht = ht
        self.annotation = annotation
        self.title = title

        if isinstance(rho, (list, tuple)):
            self.rhos = list(rho)
        else:
            self.rhos = [rho]

    def serialize(self):
        return json.dumps([self.ht, self.annotation, self.rhos, self.title]) + '\n'

    def __cmp__(self, other):
        ret = cmp(self.ht, other.ht)
        if ret == 0:
            ret = cmp(self.annotation, other.annotation)
        return ret

    def __str__(self):
        return ("%s %s %s %s" % (self.ht, self.annotation, str(self.rhos), self.title)).encode('utf-8')

class EntryFileReader(object):
    def __init__(self, file):
        self.file = file
        self.file.seek(0)
        self.iterator = iter(self.file)

    def __iter__(self):
        return self

    def next(self):
        return Entry(*json.loads(self.iterator.next().strip()))

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

class HashtagEntityGraph(object):
    def __init__(self):
        self.edges = [] # (ht, ann, [rho1, rho2, ..])
        self.inputs = []

    def partial(self):
        outfile = TemporaryFile(prefix='hashtag')
        print "Generating a new temporary file"
        self.inputs.append(outfile)

        for entry in self.edges:
            outfile.write(entry.serialize())

        self.edges = []

    def read(self, inputfile):
        for line in gzip.open(inputfile, 'r'):
            edge = json.loads(line.strip())
            hts, annotations = list(set(map(lambda x: x.lower(), edge['hts']))), edge['annotations']
            hts.sort()

            for ht in hts:
                for wid, rho, title in annotations:
                    entry = Entry('#' + ht, wid, rho, title)

                    try:
                        pos = bisect(self.edges, entry)
                        if self.edges[pos - 1] != entry:
                            self.edges.insert(pos, entry)
                    except:
                        self.edges.insert(pos, entry)

            if len(self.edges) % 1000 == 0:
                print len(self.edges)

            if len(self.edges) > 100000:
                self.partial()

        #if self.inputs:
        if True:
            self.partial()

    def write(self, outputfile, statsfile):
        if not self.inputs:
            with open(outputfile, 'w') as output:
                for edge in self.edges:
                    #output.write(msgpack.dumps(edge))
                    output.write(str(edge) + "\n")
        else:
            self.merge(outputfile, statsfile)


    def extract_nodes(self, xml, lastid):
        wikipedia_ids = {}
        prevht = ""

        for entry in heapq.merge(*(EntryFileReader(i) for i in self.inputs)):
            wikipedia_ids[int(entry.annotation)] = entry.title

            if entry.ht == prevht:
                continue

            xml.start_element('node', {'id': str(lastid)})
            xml.start_element('data', {'key': 'name'}, True)
            xml.output.characters(entry.ht)
            xml.end_element('data', True)
            xml.end_element('node')
            lastid += 1

            prevht = entry.ht

        if entry.ht != prevht:
            xml.start_element('node', {'id': str(lastid)})
            xml.start_element('data', {'key': 'name'}, True)
            xml.output.characters(entry.ht)
            xml.end_element('data', True)
            xml.end_element('node')

        print "%d Wikipedia facts" % len(wikipedia_ids)

        for id in sorted(wikipedia_ids.keys()):
            xml.start_element('node', {'id': str(id + START_ID)})
            xml.start_element('data', {'key': 'title'}, True)
            xml.output.characters(wikipedia_ids[id])
            xml.end_element('data', True)
            xml.end_element('node')

        return lastid

    def extract_edges(self, xml, stats, lastid):
        iterable = heapq.merge(*(EntryFileReader(i) for i in self.inputs))

        htid = 1
        prev = iterable.next()
        prevht = prev.ht

        for entry in iterable:
            if entry == prev:
                prev.rhos.extend(entry.rhos)
            else:
                xml.start_element('edge', {
                    "id": str(lastid),
                    "source": str(htid),
                    "target": str(prev.annotation + START_ID),
                    "label": "linked",
                })
                xml.start_element('data', {'key': 'weight'}, True)
                xml.output.characters(str(prev.rhos))
                xml.end_element('data', True)
                xml.end_element('edge')

                rhostr = ':'.join(map(str, prev.rhos))
                buff = "%d\t%d\t%s\t%s\t%s\n" % (htid, prev.annotation + START_ID, rhostr, prev.ht, prev.title)
                stats.write(buff.encode('utf-8'))

                lastid += 1
                prev = entry

            if entry.ht != prevht:
                htid += 1
                prevht = entry.ht

        rhostr = ':'.join(map(str, prev.rhos))
        buff = "%d\t%d\t%s\t%s\t%s\n" % (htid, prev.annotation + START_ID, rhostr, prev.ht, prev.title)
        stats.write(buff.encode('utf-8'))

        xml.start_element('edge', {
            "id": str(lastid),
            "source": str(htid),
            "target": str(entry.annotation + START_ID),
            "label": "linked",
        })
        xml.start_element('data', {'key': 'weight'}, True)
        xml.output.characters(str(entry.rhos))
        xml.end_element('data', True)
        xml.end_element('edge')
        lastid += 1

        return lastid

    def merge(self, outputfile, statsfile):
        with gzip.open(outputfile, 'w') as output:
            with gzip.open(statsfile, 'w') as stats:
                xml = XMLWriter(output)
                xml.start_document([
                    ('weight', 'edge', 'weight', 'string'),
                    ('name', 'node', 'name', 'string'),
                    ('title', 'node', 'title', 'string'),
                ])

                num_ht = self.extract_nodes(xml, 1)
                num_edges = self.extract_edges(xml, stats, EDGE_START_ID)

                # Per le co-occorenze rileggi il file di input

                print "Hashtags", num_ht - 1
                print "Edges", num_edges - EDGE_START_ID

                xml.end_document()

if __name__ == "__main__":
    g = HashtagEntityGraph()
    g.read(sys.argv[1])
    g.write(sys.argv[2], sys.argv[3])
