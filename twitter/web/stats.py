"""
A simple client that keep tracks of Operations/s

A possibility consists in sending out the series to a graphite instance
"""

import sys
import json
import gzip
import socket
import redis
from twitter.settings import CARBON_SERVER, CARBON_PORT

class GraphiteRelayer(object):
    def __init__(self):
        self.sock = socket.socket()
        self.sock.connect((CARBON_SERVER, CARBON_PORT))

    def relay(self, message):
        lines = []

        if message['channel'] == 'stats.ops.active':
            data = json.loads(message['data'])

            now = int(data.pop('time'))

            for key, value in data.items():
                lines.append("%s %d %d" % (key, value, now))

            msg = '\n'.join(lines) + '\n'
            self.sock.sendall(msg)

        elif message['channel'].startswith('stats.ops.'):
            data = json.loads(message['data'])
            msg = "%s %d %d\n" % (message['channel'], data[0], data[1])
            self.sock.sendall(msg)

class TimeSeriesCollector(object):
    def __init__(self, filename, is_relayer):
        self.redis = redis.Redis()

        if is_relayer:
            self.relayer = GraphiteRelayer()
        else:
            self.relayer = None

        self.filename = filename
        self.output = gzip.open(self.filename, 'w')

    def run(self):
        try:
            pubsub = self.redis.pubsub()
            pubsub.psubscribe('stats.ops.*')

            print "Listening on stats.ops.* events"

            for message in pubsub.listen():
                parsed = self.parse_message(message)

                if parsed and self.relayer:
                    self.relayer.relay(parsed)
        finally:
            self.output.close()

    def parse_message(self, message):
        if message['type'] == 'pmessage':
            print message['channel']
            parsed = {
                'channel': message['channel'],
                'data': message['data']
            }
            self.output.write(json.dumps(parsed) + '\n')
            return parsed

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("--relay", action="store_true", dest="is_relayer",
                      help="Relay all the message received to a Graphite server")
    parser.add_option("-f", "--file", dest="file",
                      help="File from which or to which store statistics")

    (options, args) = parser.parse_args()

    if options.file:
        TimeSeriesCollector(options.file, options.is_relayer).run()
    else:
        parser.print_help()