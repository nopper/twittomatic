"""
Simple web application that manages user lookup
"""

from gevent import monkey
monkey.patch_all()

import gevent
from gevent.wsgi import WSGIServer

from twitter.settings import LOOKUP_DATABASE, LOOKUP_PORT, LOOKUP_URL
from flask import Flask, Response, request
app = Flask(__name__)

import json
import anydbm
database = anydbm.open(LOOKUP_DATABASE, 'c')

@app.route('/lookup/id/<int:user_id>')
def lookup_by_userid(user_id):
    try:
        data = database['user:%d' % user_id]
        return Response(data, status=200, mimetype='application/json')
    except:
        data = json.dumps(None)
        return Response(data, status=404, mimetype='application/json')

@app.route('/lookup/add', methods=['POST'])
def insert_info():
    if request.headers['Content-Type'] == 'application/json':
        data = request.json

        user_id = data['id_str']
        screen_name = data['screen_name']

        print "Adding information for %s => %s" % (user_id, screen_name)

        database['user:%s' % user_id] = json.dumps(data)

        return Response('', status=200, mimetype='application/json')

    return Response('', status=404, mimetype='application/json')

@app.route('/')
def main():
    data = """<pre>
This is a simple service for managing user lookup informations.

There are currently stored %d lookup associations.

Two methods are present:

    - GET /lookup/id/&lt;int:user_id&gt;
    - POST /lookup/add -> {"id_str": "123", "screen_name": "name"}
<pre>
"""
    return Response(data.strip() % len(database), status=200)

def receiveInformations():
    import redis
    redis = redis.StrictRedis()

    pubsub = redis.pubsub()
    pubsub.psubscribe('extra.lookupinfos')

    for message in pubsub.listen():
        if message['type'] == 'pmessage':
            user = json.loads(message['data'])
            user_id = user['id_str']
            screen_name = user['screen_name']

            print "Adding information for %s => %s (STREAM)" % (user_id, screen_name)
            database['user:%s' % user_id] = message['data']

if __name__ == '__main__':
    gevent.spawn(receiveInformations)
    http_server = WSGIServer(('', LOOKUP_PORT), app)
    print "Starting Lookup server on port %s" % LOOKUP_URL
    http_server.serve_forever()