import json
import struct
from twitter import settings
from twitter.modules.fileutils import download, commit_file, copy_contents, new_tempfile
from twitter.backend.base import TimelineFile as BaseTimelineFile
from twitter.backend.base import FollowerFile as BaseFollowerFile

def compare_tweets(tweet1, tweet2):
    return cmp(int(tweet1['id_str']), int(tweet2['id_str']))

def load_extents(file):
    first, last, total = None, None, 0
    try:
        iterable = iter(file)
        first = json.loads(iterable.next().strip())
        total += 1

        for count, line in enumerate(iterable):
            pass

        last = json.loads(line.strip())
        total += count + 1
    except:
        pass

    return first, last, total

class TimelineFile(BaseTimelineFile):
    def __init__(self, user_id):
        BaseTimelineFile.__init__(self, user_id)

        # Obtain a copy of the file and read all the contents in memory
        self.local_copy = download(user_id, 'twt')

        self.local_copy.seek(0, 0)
        self.first, self.last, self.total = load_extents(self.local_copy)

        self.local_copy.seek(0, 2)
        self.update_buff = []

    def get_first(self):
        return self.first

    def get_last(self):
        return self.last

    def get_total(self):
        return self.total

    def add_tweet(self, tweet):
        assert isinstance(tweet, dict)
        # This tweet is an update to the timeline
        if self.first and compare_tweets(tweet, self.first) > 0:
            self.update_buff.append(tweet)
        else:
            self.local_copy.write("%s\n" % json.dumps(tweet, sort_keys=True))

    def commit(self):
        if self.update_buff:
            output = new_tempfile(self.user_id, 'twt')
            self.update_buff.sort(compare_tweets, reverse=True)

            for tweet in self.update_buff:
                output.write("%s\n" % json.dumps(tweet, sort_keys=True))

            self.local_copy.seek(0, 0)
            copy_contents(self.local_copy, output)
        else:
            output = self.local_copy

        output.seek(0, 0)
        commit_file(output, self.user_id, 'twt')

class FollowerFile(BaseFollowerFile):
    def __init__(self, user_id, start_line=1):
        BaseFollowerFile.__init__(self, user_id)

        # Obtain a copy of the file and read all the contents in memory
        self.local_copy = download(user_id, 'fws')
        self.item_size = struct.calcsize("!Q")

        self.local_copy.seek(0, 2)
        self.length = self.local_copy.tell() / self.item_size

    def add_follower(self, user_id):
        self.local_copy.seek(0, 2)
        self.local_copy.write(struct.pack("!Q", int(user_id)))

    def __len__(self):
        return self.length

    def __getitem__(self, value):
        if value < 0 or value >= self.length:
            raise IndexError

        position = value * self.item_size
        self.local_copy.seek(position, 0)
        return str(struct.unpack("!Q", self.local_copy.read(self.item_size))[0])

    def commit(self):
        self.local_copy.seek(0, 0)
        commit_file(self.local_copy, self.user_id, 'fws')
