"""
Simple indexer module that listens on tweet events and report them
to an ElasticSearch instance. This is usefull for on the fly indexing
"""

import json
import time
import redis

# Elastic search imports
from pyes import *
from pyes.helpers import SettingsBuilder
from pyes.mappings import *

INDEX_NAME = "twitter"
DOCUMENT_TYPE = "tweet-type"

def get_conn(*args, **kwargs):
    return ES()

def init_index():
    conn = get_conn()

    mapping = {
        'text': {
            'boost': 1.0,
            'index': 'analyzed',
            'store': 'yes',
            'type': 'string',
            "term_vector": 'with_positions_offsets'
        },
        'screen_name': {
            'boost': 1.0,
            'store': 'yes',
            'type': 'string',
        },
        'created_at': {
            'boost': 1.0,
            'store': 'yes',
            'type': 'date',
            'format' : 'yyyy/MM/dd HH:mm:ss',
        },
        'id_str': {
            'store': 'yes',
            'type': 'long'
        },
        'geo' : {
            'properties' : {
                'location' : {
                    'type' : 'geo_point'
                }
            }
        },
        'hashtags' : {
            'type' : 'string',
            'index_name' : 'hashtags'
        },
    }

    try:
        conn.indices.delete_index(INDEX_NAME)
    except Exception, e:
        pass

    conn.indices.create_index(INDEX_NAME)
    conn.indices.put_mapping(DOCUMENT_TYPE, {'properties': mapping}, [INDEX_NAME])

    print "Index successfully created"

def receiveInformations():
    r = redis.StrictRedis()
    conn = get_conn()

    pubsub = r.pubsub()
    pubsub.psubscribe('events.timeline.*')
    insertions = []

    for message in pubsub.listen():
        # yyyy/MM/dd HH:mm:ss
        if message['type'] == 'pmessage':
            tweet = json.loads(message['data'])
            hts = map(lambda x: x['text'], tweet['entities']['hashtags'])
            created_at = time.strftime("%Y/%m/%d %H:%M:%S", time.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y"))

            document = {
                "screen_name": tweet['user']['screen_name'],
                "text": tweet['text'],
                "id_str": int(tweet['id_str']),
                "created_at": created_at,
            }

            if hts:
                document['hashtags'] = hts

            if tweet['geo']:
                coordinates = tweet['geo']['coordinates']
                document['geo'] = {'location': coordinates}

            conn.index(document, INDEX_NAME, DOCUMENT_TYPE)

if __name__ == '__main__':
    init_index()
    receiveInformations()