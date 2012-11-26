"""
Simple web monitor application
"""

from gevent import monkey
monkey.patch_all()

import gevent
from gevent.wsgi import WSGIServer

import os
import json
import redis

from twitter.settings import LOOKUP_DATABASE, MONITOR_PORT, MONITOR_URL
from flask import Flask, Response, request, render_template

TEMPLATE_ROOT = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, template_folder=TEMPLATE_ROOT)

r = redis.StrictRedis()
statistics = {}

@app.route('/')
def main():
    keywords = [
        'timeline.total_included',
        'timeline.total_fetched',
        'follower.total_fetched',
        'analyzer.total_included',
        'analyzer.total_fetched',
        'update.total_included',
        'update.total_fetched',

        'stats.worker.ongoing.timeline',
        'stats.worker.ongoing.follower',
        'stats.worker.ongoing.analyzer',
        'stats.worker.ongoing.update',

        'stats.worker.completed.timeline',
        'stats.worker.completed.follower',
        'stats.worker.completed.analyzer',
        'stats.worker.completed.update',
    ]

    pipe = r.pipeline()

    for key in keywords:
        pipe.get(key)

    stats = dict(zip(keywords, pipe.execute()))
    
    first_five = r.lrange('stream', 0, 5)
    first_five_error = r.lrange('error_stream', 0, 5)

    return render_template('templates/monitor_main.html',
        statistics=stats,
        stream=first_five,
        error_stream=first_five_error,
    )

def receiveInformations():
    import redis
    redis = redis.StrictRedis()

    pubsub = redis.pubsub()
    pubsub.psubscribe('stats.*')

    for message in pubsub.listen():
        if message['type'] == 'pmessage':
            statistics = json.loads(message['data'])

if __name__ == '__main__':
    # gevent.spawn(receiveInformations)
    # http_server = WSGIServer(('', MONITOR_PORT), app)
    # print "Starting Monitor server on port %s" % MONITOR_URL
    # http_server.serve_forever()
    app.run(debug=True, port=MONITOR_PORT)