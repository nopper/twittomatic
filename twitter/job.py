from collections import namedtuple

class TwitterJob(namedtuple('TwitterJob', 'operation, user_id, cursor')):
    TIMELINE_OP = 'T'
    FOLLOWER_OP = 'F'
    ANALYZER_OP = 'A'
    UPDATE_OP   = 'U'
    
    @classmethod
    def deserialize(self, str):
        if str is None:
            return None
        operation, user_id, cursor = str.split(',', 2)
        return TwitterJob(operation, int(user_id), int(cursor))

    @classmethod
    def serialize(self, job):
        return '%s,%d,%d' % (job.operation, job.user_id, job.cursor)
