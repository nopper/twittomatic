from collections import namedtuple

class Stats:
    TIMELINE_TOTAL_INCLUDED = 'timeline.total_included'
    TIMELINE_TOTAL_FETCHED  = 'timeline.total_fetched'

    FOLLOWER_TOTAL_FETCHED  = 'follower.total_fetched'

    ANALYZER_TOTAL_INCLUDED = 'analyzer.total_included'
    ANALYZER_TOTAL_FETCHED  = 'analyzer.total_fetched'

    UPDATE_TOTAL_INCLUDED   = 'update.total_included'
    UPDATE_TOTAL_FETCHED    = 'update.total_fetched'

    TIMELINE_ONGOING        = 'stats.worker.ongoing.timeline'
    FOLLOWER_ONGOING        = 'stats.worker.ongoing.follower'
    ANALYZER_ONGOING        = 'stats.worker.ongoing.analyzer'
    UPDATE_ONGOING          = 'stats.worker.ongoing.update'

    TIMELINE_COMPLETED      = 'stats.worker.completed.timeline'
    FOLLOWER_COMPLETED      = 'stats.worker.completed.follower'
    ANALYZER_COMPLETED      = 'stats.worker.completed.analyzer'
    UPDATE_COMPLETED        = 'stats.worker.completed.update'


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
