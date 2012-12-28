class TimelineFile(object):
    def __init__(self, user_id):
        self.user_id = user_id

    def add_tweet(self, tweet):
        pass

    def get_first(self):
        pass

    def get_last(self):
        pass

    def get_total(self):
        pass

class FollowerFile(object):
    def __init__(self, user_id):
        self.user_id = user_id

    def add_followers(self, user_ids):
        pass

    def get_processed(self, cursor):
        return 0

    def followers(self, cursor="0"):
        raise StopIteration