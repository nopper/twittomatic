import json

from twitter import settings
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException

from twitter.backend.base import TimelineFile as BaseTimelineFile
from twitter.backend.base import FollowerFile as BaseFollowerFile

print "Initializing connnection pool..."

POOL = ConnectionPool(settings.CASSANDRA_KEYSPACE)
FOLLOWERS = ColumnFamily(POOL, 'Followers')
TIMELINE = ColumnFamily(POOL, 'Timeline')

class TimelineFile(BaseTimelineFile):
    def __init__(self, user_id):
        BaseTimelineFile.__init__(self, user_id)

    def get_first(self):
        dct = TIMELINE.get(str(self.user_id), column_count=1, column_reversed=True)
        tweet = json.loads(dct[dct.keys()[0]])

        print "RETURING", tweet['id_str'], tweet['text']
        return tweet

    def get_last(self):
        dct = TIMELINE.get(str(self.user_id), column_count=1)
        tweet = json.loads(dct[dct.keys()[0]])

        print "RETURING", tweet['id_str'], tweet['text']
        return tweet

    def get_total(self):
        return TIMELINE.get_count(str(self.user_id))

    def add_tweet(self, tweet):
        assert isinstance(tweet, dict)

        TIMELINE.insert(str(self.user_id), {int(tweet['id_str']): json.dumps(tweet, sort_keys=True)})

    def commit(self):
        pass

class FollowerFile(BaseFollowerFile):
    # We could simply have used a fucking packed 64bit numbers array
    # but we are for readability
    def __init__(self, user_id, start_line=1):
        BaseFollowerFile.__init__(self, user_id)
        try:
            dct = FOLLOWERS.get(str(self.user_id), column_count=1, column_reversed=True)
            self.length = int(dct.keys()[0]) + 1
        except NotFoundException:
            self.length = 0

    def add_follower(self, user_id):
        FOLLOWERS.insert(str(self.user_id), {int(self.length): str(user_id)})
        self.length += 1

    def __len__(self):
        try:
            dct = FOLLOWERS.get(str(self.user_id), column_count=1, column_reversed=True)
            self.length = int(dct.keys()[0]) + 1
        except NotFoundException:
            self.length = 0

        return self.length

    def __getitem__(self, value):
        if value < 0 or value >= self.length:
            raise IndexError

        dct = FOLLOWERS.get(str(self.user_id), [value])
        return dct[value]

    def commit(self):
        pass