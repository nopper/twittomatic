import sys
from json import loads
from requests import post

class AnnotationExtractor(object):
    def __init__(self):
        self.requests = 0

    def annotate(self, text, is_tweet=False, raw=False):
        payload = {
            'key': '45fgh00',
            'text': text,
            'lang': 'it',
            'epsilon': '0.45',
        }

        if is_tweet:
            payload['tweet'] = 'true'

        r = post('http://localhost:8080/tag', params=payload)

        try:
            json = loads(r.text)
        except Exception, exc:
            print >> sys.stderr, "Error tagging:", text
            return []

        if raw:
            def threshold_rho(annotation):
                return float(annotation.get('rho', 0)) > 0.10

            return filter(threshold_rho, json.get('annotations', []))

        def convert(annotation):
            return (annotation['id'], float(annotation['rho']), annotation.get('title', ''))

        self.requests += 1
        annotations = filter(lambda x: x[1] >= 0.15,
                             map(convert, json.get('annotations', [])))
        return annotations

