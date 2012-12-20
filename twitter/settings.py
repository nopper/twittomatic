import os
import sys
from twitter.const import *

# Number of max clients supported by the tracker
MAX_CLIENTS = 200

# Information about the Job tracker instance, which is the master
JT_HOSTNAME = 'localhost'
JT_PORT = 8000

# Kill the worker (SIGKILL) on master's disconnection
FORCE_KILL_ON_DISCONNECT = True
TESTING = False

if TESTING:
    import fakeredis
    REDIS_CLASS = fakeredis.FakeRedis
else:
    import redis
    REDIS_CLASS = redis.StrictRedis

import job
JOB_CLASS = job.TwitterJob

LOG_DIRECTORY = os.path.join(os.path.expanduser('~'), 'twitter-logs')
TEMPORARY_DIRECTORY = os.path.join(os.path.expanduser('~'), 'twitter-temp')
OUTPUT_DIRECTORY = os.path.join(os.path.expanduser('~'), 'twitter-ds')
LOOKUP_DATABASE = os.path.join(os.path.expanduser('~'), 'twitter-lookup')

# The lookup service is just used to look up user_id to screen_names
# It actually returns all the user information. It's not used directly
# by using HTTP requests, but it just waits events on the pubsub interface
# exposed by redis.

LOOKUP_PORT = 9797
LOOKUP_URL = 'http://localhost:%d' % LOOKUP_PORT
LOOKUP_URL_ADD = LOOKUP_URL + '/lookup/add'

# The monitor application is just used for monitoring the crawler status

MONITOR_PORT = 9898
MONITOR_URL = 'http://localhost:%d' % MONITOR_PORT

# Just used for testing

TWITTER_MAXATTEMPTS = 4
TWITTER_TIMEOUT_FACTOR = 2.35 # A maximum timeout of 2.35 ** 4
TWITTER_TOOMANY_SLEEP = 60

TWITTER_URL = "http://api.twitter.com/1/" # This requires no authentication
# TWITTER_URL = "http://localhost:5000/1/"

if not os.path.exists(OUTPUT_DIRECTORY):
    print "Directory %s does not exist. Exiting." % OUTPUT_DIRECTORY
    sys.exit(-1)

if not os.path.exists(LOG_DIRECTORY):
    print "Directory %s does not exist. Exiting." % LOG_DIRECTORY
    sys.exit(-1)

if not os.path.exists(TEMPORARY_DIRECTORY):
    print "Directory %s does not exist. Exiting." % TEMPORARY_DIRECTORY
    sys.exit(-1)

# Settings used by redis
FRONTIER_NAME = 'italian_followers'

USERS_SEEDS     = 'users.seeds'
USERS_DISCARDED = 'users.discarded' # Not entirely necessary. Just used as a cache mechanism
USERS_SELECTED  = 'users.selected'

LOG_LIST = 'logger'
LOG_SCROLLBACK = 300

# Used by the stats server module
CARBON_SERVER = '127.0.0.1'
CARBON_PORT = 2003

GRAPHITE_URL = 'http://localhost:8080'

USE_HDFS = False
HDFS_DIRECTORY = '/twitter/'

STORAGE_CLASS = 'file' # You can choose betweet file and cassandra
CASSANDRA_KEYSPACE = 'crawler'
CASSANDRA_POOL = ['localhost:9160']

USE_COMPRESSION = True

ELASTICSEARCH_URL = 'http://localhost:9200'