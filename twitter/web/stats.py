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

class GraphiteCollector(object):
    def __init__(self, inputfile):
        self.sock = socket.socket()
        self.sock.connect((CARBON_SERVER, CARBON_PORT))
        increment = 0

        with gzip.open(inputfile, 'r') as input:
            for line in input:
                lines = []
                message = json.loads(line)

                # This a summary message printed usually every 5 seconds
                if message['channel'] == 'stats.ops.active':
                    data = json.loads(message['data'])

                    now = int(data.pop('time'))

                    for key, value in data.items():
                        lines.append("%s %d %d" % (key, value, now))

                    msg = '\n'.join(lines) + '\n'
                    print "Sending out", msg
                    self.sock.sendall(msg)

                elif message['channel'].startswith('stats.ops.'):
                    data = json.loads(message['data'])
                    msg = "%s %d %d\n" % (message['channel'], data[0], data[1])
                    self.sock.sendall(msg)


class TimeSeriesCollector(object):
    def __init__(self, filename):
        self.filename = filename
        self.redis = redis.Redis()

    def run(self):
        pubsub = self.redis.pubsub()
        with gzip.open(self.filename, 'w') as output:
            pubsub.psubscribe('stats.ops.*')

            for message in pubsub.listen():
                print message
                if message['type'] == 'pmessage':
                    output.write(json.dumps({
                        'channel': message['channel'],
                        'data': message['data']
                    }) + '\n')

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-l", action="store_true", dest="load",
                     help="Import statistics collected to a Graphite server")
    parser.add_option("-s", action="store_true", dest="save",
                     help="Save the statistics collected to a gzip file")
    parser.add_option("-f", "--file", dest="file",
                      help="File from which or to which store statistics")

    (options, args) = parser.parse_args()
 
    if options.file and options.load:
        GraphiteCollector(options.file)
    elif options.file and options.save:
        TimeSeriesCollector(options.file).run()
    else:
        parser.print_help()