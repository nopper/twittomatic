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

from twitter.settings import LOOKUP_DATABASE, MONITOR_PORT, MONITOR_URL, LOG_LIST, LOG_SCROLLBACK, GRAPHITE_URL, ELASTICSEARCH_URL
from flask import Flask, Response, request, render_template

TEMPLATE_ROOT = os.path.abspath(os.path.dirname(__file__))
app = Flask(__name__, template_folder=TEMPLATE_ROOT)

r = redis.StrictRedis()
statistics = {}

@app.route('/inspect')
def inspect():
    return render_template('templates/monitor_map.html', elasticsearch_url=ELASTICSEARCH_URL)

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

    results = r.mget(keywords)
    stats = dict(zip(keywords, results))

    timeline = int(stats['stats.worker.ongoing.timeline'])
    follower = int(stats['stats.worker.ongoing.follower'])
    analyzer = int(stats['stats.worker.ongoing.analyzer'])
    update = int(stats['stats.worker.ongoing.update'])

    total = float(timeline + follower + analyzer + update)

    if total > 0:
        timeline = (timeline / total) * 100
        follower = (follower / total) * 100
        analyzer = (analyzer / total) * 100
        update = (update / total) * 100
    else:
        timeline = follower = analyzer = update = 0

    first_five = r.lrange('stream', 0, 5)
    first_five_error = r.lrange('error_stream', 0, 5)

    logs = r.lrange(LOG_LIST, 0, LOG_SCROLLBACK)
    logs.reverse()

    return render_template('templates/monitor_main.html',
        statistics=stats,
        stream=first_five,
        error_stream=first_five_error,
        logs=logs,
        timeline_percentage=timeline,
        follower_percentage=follower,
        analyze_percentage=analyzer,
        update_percentage=update,
        graphite_url=GRAPHITE_URL
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