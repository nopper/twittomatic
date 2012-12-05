import gevent

from gevent import monkey
monkey.patch_socket()

import time
import gzip
import redis
import signal

from gevent import socket

from twitter.job import Stats
from twitter.settings import CARBON_SERVER, CARBON_PORT

r = redis.StrictRedis()
output = None
event_counter = 0

sock = socket.socket()
sock.connect((CARBON_SERVER, CARBON_PORT))

MONITORED_VALUES = [
    Stats.TIMELINE_TOTAL_INCLUDED,
    Stats.TIMELINE_TOTAL_FETCHED,

    Stats.FOLLOWER_TOTAL_FETCHED,

    Stats.ANALYZER_TOTAL_INCLUDED,
    Stats.ANALYZER_TOTAL_FETCHED,

    Stats.UPDATE_TOTAL_INCLUDED,
    Stats.UPDATE_TOTAL_FETCHED,

    Stats.TIMELINE_ONGOING,
    Stats.FOLLOWER_ONGOING,
    Stats.ANALYZER_ONGOING,
    Stats.UPDATE_ONGOING,

    Stats.TIMELINE_COMPLETED,
    Stats.FOLLOWER_COMPLETED,
    Stats.ANALYZER_COMPLETED,
    Stats.UPDATE_COMPLETED,
]

def stats_polling():
    global event_counter

    while True:
        now = time.time()
        values = r.mget(MONITORED_VALUES)

        lines = []
        ts = int(now)

        for label, value in zip(MONITORED_VALUES, values):
            try:
                value = int(value)
            except:
                value = 0

            lines.append("%s %d %d" % (label, value, ts))
            event_counter += 1

            if event_counter % 100 == 0:
                print "Events collected %d" % event_counter

        msg = '\n'.join(lines) + '\n'

        sock.send(msg)
        output.write(msg)

        gevent.sleep(abs(time.time() - (now + 1)))

def parse_message(message):
    if message['type'] == 'pmessage':
        parsed = {
            'channel': message['channel'],
            'data': message['data']
        }
        return parsed

def stats_receiver():
    global event_counter

    pubsub = r.pubsub()
    pubsub.psubscribe('stats.ops.*')

    print "Listening on stats.ops.* events"

    for message in pubsub.listen():
        msg = parse_message(message)

        if msg:
            data = json.loads(msg['data'])
            msg = "%s %d %d\n" % (msg['channel'], data[0], data[1])
            sock.sendall(msg)
            output.write(msg)

            event_counter += 1

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-f", "--file", dest="file",
                      help="File from which or to which store statistics")

    (options, args) = parser.parse_args()

    if options.file:
        output = gzip.open(options.file, 'w')

        def close_file():
            output.close()

        gevent.signal(signal.SIGTERM, close_file)
        gevent.signal(signal.SIGINT, close_file)

        gevent.joinall([
            gevent.spawn(stats_polling),
            gevent.spawn(stats_receiver),
        ])
    else:
        parser.print_help()