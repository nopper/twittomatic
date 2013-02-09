import gzip
import json
import datetime
from contextlib import contextmanager

@contextmanager
def profiled(str):
    start = datetime.datetime.now()
    yield start
    diff = datetime.datetime.now() - start
    print(str % diff)

def iterate_anchors(anchorfile):
    with gzip.open(anchorfile, 'r') as inputfile:
        for line in inputfile:
            anchor = json.loads(line.strip())

            label = anchor['anchor']
            pages = anchor['pages']

            yield label.lower(), pages

def iterate_mappings(titlesfile):
    with gzip.open(titlesfile, 'r') as inputfile:
        for line in inputfile:
            page = json.loads(line.strip())
            yield page['id'], page['name'], page['title'], page['length']

def iterate_templates(templatefile):
    with gzip.open(templatefile, 'r') as inputfile:
        for line in inputfile:
            page = json.loads(line.strip())
            yield page['id'], page['templates']
