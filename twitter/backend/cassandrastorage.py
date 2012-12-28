import json

from twitter import settings
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException

from twitter.backend.base import TimelineFile as BaseTimelineFile
from twitter.backend.base import FollowerFile as BaseFollowerFile

print "Initializing connnection pool..."

POOL = ConnectionPool(settings.CASSANDRA_KEYSPACE, settings.CASSANDRA_POOL, timeout=2)
FOLLOWERS = ColumnFamily(POOL, 'Followers')
USERTIMELINE = ColumnFamily(POOL, 'UserTimeline')
TIMELINE = ColumnFamily(POOL, 'Timeline')
COUNTERS = ColumnFamily(POOL, 'Counters')

class TimelineFile(BaseTimelineFile):
    def __init__(self, user_id):
        BaseTimelineFile.__init__(self, user_id)

    def get_first(self):
        dct = USERTIMELINE.get(self.user_id, column_count=1, column_reversed=True)

        tweet_id = dct[dct.keys()[0]]
        tweet = TIMELINE.get(str(tweet_id))

        return json.loads(tweet['tweet'])

    def get_last(self):
        dct = USERTIMELINE.get(self.user_id, column_count=1)

        tweet_id = dct[dct.keys()[0]]
        tweet = TIMELINE.get(str(tweet_id))

        return json.loads(tweet['tweet'])

    def get_total(self):
        return USERTIMELINE.get_count(self.user_id)

    def add_tweets(self, tweets):
        assert isinstance(tweets, list)

        def batch(iterable, n=100):
           l = len(iterable)
           for ndx in range(0, l, n):
               yield iterable[ndx:min(ndx + n, l)]

        for seq in batch(tweets, 10):
            ids_dct = dict(map(lambda x: (int(x['id_str']), 0), tweets))
            USERTIMELINE.insert(self.user_id, ids_dct)
            TIMELINE.batch_insert(
                dict(map(lambda x: (x['id_str'], {"tweet": json.dumps(x)}), seq))
            )

    def commit(self):
        pass

class FollowerFile(BaseFollowerFile):
    def __init__(self, user_id, start_line=1):
        BaseFollowerFile.__init__(self, user_id)

    def add_followers(self, followers):
        def batch(iterable, n=100):
           l = len(iterable)
           for ndx in range(0, l, n):
               yield dict(zip(iterable[ndx:min(ndx + n, l)], (0 for i in xrange(min(ndx + n, l)))))

        for dct in batch(followers):
            try:
                present = FOLLOWERS.get(self.user_id, dct)
            except:
                present = {}

            FOLLOWERS.insert(self.user_id, dct)
            COUNTERS.add(str(self.user_id), "follower_count", len(dct) - len(present))

    def __len__(self):
        try:
            return COUNTERS.get(str(self.user_id), ["follower_count"])['follower_count']
        except:
            return 0

    def get_processed(self, cursor):
        try:
            num_processed, prev_user = map(int, cursor.split('|', 1))
            return num_processed
        except:
            return 0

    def followers(self, cursor="0"):

        if cursor == '0':
            prev_user = 0
            num_processed = 0
        else:
            num_processed, prev_user = map(int, cursor.split('|', 1))

        while True:
            ret = FOLLOWERS.get(self.user_id, column_start=prev_user, column_count=100)

            if len(ret) <= 1:
                break

            for follower_id in ret:
                if follower_id == prev_user:
                    continue

                num_processed += 1

                yield follower_id, '|'.join(map(str, [num_processed, follower_id]))

                prev_user = follower_id

        raise StopIteration

    def commit(self):
        pass
