import os
import math
import json
import plyvel

from struct import pack
from array import array

class Entity(object):
    def __init__(self, parent, wid, title):
        self.wid = wid
        self.title = title
        self.catalog = parent

        self.indegree = self.catalog.indegree(self)
        self.outdegree = self.catalog.outdegree(self)

    def related(self, other):
        return self.catalog.mw_relatedness(self, other)

    def is_redirect(self):
        return len(self.indegree) == 0 and len(self.outdegree) == 1

    def get_redirect(self):
        wid = list(self.outdegree)[0]
        return Entity(self.catalog, wid, self.catalog.get_title(wid))

class Catalog(object):
    def __init__(self, datapath=''):
        self.titles = plyvel.DB(os.path.join(datapath, 'wikipedia-titles'))
        self.in_deg = plyvel.DB(os.path.join(datapath, 'wikipedia-indeg'))
        self.out_deg = plyvel.DB(os.path.join(datapath, 'wikipedia-outdeg'))
        self.portals = plyvel.DB(os.path.join(datapath, 'wikipedia-portals'))
        self.length = 1743776
        # TODO: Export this information in the stats

        self.cache = {}

    def get_portals(self, wid):
        result = self.portals.get(pack("!I", wid))

        if result:
            return json.loads(result)
        else:
            return []

    def get_title(self, wid):
        return self.titles.get('id:%s' % pack("!I", wid))

    def get_entity(self, title, wid=None):
        if wid is None:
            wid = int(self.titles.get("title:%s" % title.lower().replace(' ', '_').encode('utf8')))
        if title is '':
            title = self.get_title(wid)

        entity = Entity(self, wid, title)

        if entity.is_redirect():
            return entity.get_redirect()

        return entity

    def mw_relatedness(self, entity_a, entity_b):
        A = self.indegree(entity_a)
        B = self.indegree(entity_b)
        AB = A.intersection(B)
        W = self.length

        A = len(A)
        B = len(B)
        AB = len(AB)

        if AB == 0:
            return 0

        return (math.log(max(A, B)) - math.log(AB)) / (math.log(W) - math.log(min(A, B)))

    def indegree(self, entity):
        return self.degree_get(entity, indeg=True)

    def outdegree(self, entity):
        return self.degree_get(entity, indeg=False)

    def degree_get(self, entity, indeg=False):
        if indeg:
            db = self.in_deg
        else:
            db = self.out_deg

        result = self.cache.get(entity.wid, None)

        if result is not None:
            return result

        value = pack("!I", entity.wid)
        ret = db.get(value)

        if not ret:
            result = set()
        else:
            result = set(array("i", ret).tolist())

        self.cache[entity.wid] = result

        return result


if __name__ == "__main__":
    catalog = Catalog()
    print catalog.get_entity('Roma').related(catalog.get_entity('Corigliano'))
    print catalog.get_entity('Armonium').related(catalog.get_entity('Musica'))
    print catalog.get_entity('Armonium').related(catalog.get_entity('Strumento_musicale'))
    print catalog.get_entity('Enrico Fermi').related(catalog.get_entity('Premio Nobel'))
    print catalog.get_entity('Premio nobel per la chimica').related(catalog.get_entity('Fisica nucleare'))
    print catalog.get_entity('Premio nobel').related(catalog.get_entity('Fisica nucleare'))
