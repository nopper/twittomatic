import os
import time
import settings
from crawler.master import *
from twitter.const import *
from twitter.job import TwitterJob
from twitter.modules.redislogger import RedisLogObserver

import gzip
from tempfile import NamedTemporaryFile

from redis import WatchError
from twisted.python import log
from twisted.python.logfile import DailyLogFile

TRANSFORM_TIMELINE = 1
TRANSFORM_FOLLOWER = 2
TRANSFORM_ANALYZER = 4
# The updater does not provide any transformation

class TwitterJobTrackerFactory(JobTrackerFactory):
    def __init__(self, *args, **kwargs):
        self.initialized = False
        self.options = kwargs.pop('options')

        JobTrackerFactory.__init__(self, *args, **kwargs)

        if self.options.seeds_file:
            self.loadSeedsFrom(self.options.seeds_file)

        self.transformation = 0

        if self.options.timeline:
            self.transformation |= TRANSFORM_TIMELINE
        if self.options.follower:
            self.transformation |= TRANSFORM_FOLLOWER
        if self.options.analyze:
            self.transformation |= TRANSFORM_ANALYZER

        if self.options.timeline:
            self.loadTargetsFrom(self.options.stream_file, TwitterJob.TIMELINE_OP)
        elif self.options.follower:
            self.loadTargetsFrom(self.options.stream_file, TwitterJob.FOLLOWER_OP)
        elif self.options.analyze:
            self.loadTargetsFrom(self.options.stream_file, TwitterJob.ANALYZER_OP)
        elif self.options.update:
            self.loadTargetsFrom(self.options.stream_file, TwitterJob.UPDATE_OP)
        else:
            log.msg("No input file provided. Assuming the stream is already available")

        self.initializeStatistics({
            'timeline.total_included': 0,
            'timeline.total_fetched': 0,
            'follower.total_fetched': 0,
            'analyzer.total_included': 0,
            'analyzer.total_fetched': 0,
            'update.total_included': 0,
            'update.total_fetched': 0,

            'stats.worker.ongoing.timeline': 0,
            'stats.worker.ongoing.follower': 0,
            'stats.worker.ongoing.analyzer': 0,
            'stats.worker.ongoing.update': 0,

            'stats.worker.completed.timeline': 0,
            'stats.worker.completed.follower': 0,
            'stats.worker.completed.analyzer': 0,
            'stats.worker.completed.update': 0,
        })

    def recoverFromCrash(self):
        """
        Here we need to remove all the keys from redis that are listed as assigned:
        and push back from ongoing to the stream all the works
        """
        abort = False

        if not options.want_recovery:
            log.msg("A crash condition was detected but the --recovery flag is not enabled")
            sys.exit(0)
        else:
            log.msg("A crash condition was detected.")

        with self.redis.pipeline() as pipe:
            try:
                pipe.watch('ongoing')
                pipe.watch('master.refcount')
                assigned_jobs = pipe.smembers('ongoing')
                assigned_keys = pipe.keys('assigned:*')

                if assigned_keys:
                    values = set(pipe.mget(assigned_keys))
                else:
                    values = set([])

                if values != assigned_jobs:
                    log.msg('Assigned jobs does not match the ongoing set')
                    abort = True
                else:
                    pipe.multi()

                    for job in assigned_jobs:
                        log.msg("Reinserting %s in stream queue" % job)
                        pipe.lpush('stream', job)

                    pipe.delete('ongoing')
                    if assigned_keys:
                        pipe.delete(*assigned_keys)
                    pipe.delete('master.refcount')
                    pipe.set('stats.worker.ongoing.timeline', 0)
                    pipe.set('stats.worker.ongoing.follower', 0)
                    pipe.set('stats.worker.ongoing.analyzer', 0)
                    pipe.set('stats.worker.ongoing.update', 0)
                    pipe.execute()

                    log.msg("%d jobs successfully recovered" % len(assigned_jobs))

            except WatchError, e:
                log.msg('A watched variable was modified. Aborting')
                abort = True

            except Exception, e:
                log.msg("Unknown error condition")
                log.err()
                abort = True

        if abort:
            log.msg("Aborting recovery as requested")
            sys.exit(0)

    def initializeStatistics(self, attrs):
        self.initialized = True

        for k, v in attrs.items():
            if self.redis.get(k) is None:
                self.redis.set(k, v)

    def loadSeedsFrom(self, inputfile):
        self.redis.delete(settings.USERS_SEEDS)

        with open(inputfile, 'r') as input:
            for line in input:
                if line.strip():
                    try:
                        user_id = int(line.strip())
                        self.redis.sadd(settings.USERS_SEEDS, user_id)
                    except:
                        pass

        log.msg("Successfully loaded %d users into USER_SEEDS" % self.redis.scard(settings.USERS_SEEDS))

    def loadTargetsFrom(self, targetfile, type):
        if not targetfile:
            return

        stream_len = self.redis.llen('stream')

        if stream_len and stream_len == 0:
            raise Exception("Trying to load users into stream while the stream is non-empty")

        if type == TwitterJob.TIMELINE_OP:
            fmt = 'T,%d,0'
        elif type == TwitterJob.FOLLOWER_OP:
            fmt = 'F,%d,-1'
        elif type == TwitterJob.ANALYZER_OP:
            fmt = 'A,%d,1'
        elif type == TwitterJob.UPDATE_OP:
            fmt = 'U,%d,0'
        else:
            raise Exception("Unknown type")

        with open(targetfile, 'r') as input:
            for line in input:
                if line.strip():
                    try:
                        user_id = int(line.strip())
                        self.redis.lpush('stream', fmt % user_id)
                    except:
                        pass

        log.msg("Successfully loaded %d users into stream" % self.redis.llen('stream'))

    def assignJobTo(self, client):
        type, job = JobTrackerFactory.assignJobTo(self, client)

        # Just used to keep track of some general statistics
        if type == TYPE_JOB:
            deserialized = TwitterJob.deserialize(job)

            if deserialized.operation == TwitterJob.TIMELINE_OP:
                self.redis.incr('stats.worker.ongoing.timeline')
            elif deserialized.operation == TwitterJob.FOLLOWER_OP:
                self.redis.incr('stats.worker.ongoing.follower')
            elif deserialized.operation == TwitterJob.ANALYZER_OP:
                self.redis.incr('stats.worker.ongoing.analyzer')
            elif deserialized.operation == TwitterJob.UPDATE_OP:
                self.redis.incr('stats.worker.ongoing.update')

        return type, job

    def manageLostClient(self, client):
        if self.clients[client] == WORKER_WORKING:
            deserialized = TwitterJob.deserialize(self.assigned_jobs[client])

            if deserialized.operation == TwitterJob.TIMELINE_OP:
                self.redis.decr('stats.worker.ongoing.timeline')
            elif deserialized.operation == TwitterJob.FOLLOWER_OP:
                self.redis.decr('stats.worker.ongoing.follower')
            elif deserialized.operation == TwitterJob.ANALYZER_OP:
                self.redis.decr('stats.worker.ongoing.analyzer')
            elif deserialized.operation == TwitterJob.UPDATE_OP:
                self.redis.decr('stats.worker.ongoing.update')

        return JobTrackerFactory.manageLostClient(self, client)

    def summary(self):
        if self.initialized:
            log.msg('=== STATISTICS ===')
            JobTrackerFactory.summary(self)

            otstats, ofstats, oastats, oustats = \
                int(self.redis.get('stats.worker.ongoing.timeline')), \
                int(self.redis.get('stats.worker.ongoing.follower')), \
                int(self.redis.get('stats.worker.ongoing.analyzer')), \
                int(self.redis.get('stats.worker.ongoing.update'))

            ctstats, cfstats, castats, custats = \
                int(self.redis.get('stats.worker.completed.timeline')), \
                int(self.redis.get('stats.worker.completed.follower')), \
                int(self.redis.get('stats.worker.completed.analyzer')), \
                int(self.redis.get('stats.worker.completed.update'))

            log.msg("STATS: ONGOING:   Timeline: %d Follower: %d Analyzer: %d Update: %d" % \
                    (otstats, ofstats, oastats, oustats))
            log.msg("STATS: COMPLETED: Timeline: %d Follower: %d Analyzer: %d Update: %d" % \
                    (ctstats, cfstats, castats, custats))

            self.redis.publish('stats.ops.active', json.dumps({
                'time': time.time(),

                'stats.worker.ongoing.timeline': otstats,
                'stats.worker.ongoing.follower': ofstats,
                'stats.worker.ongoing.analyzer': oastats,
                'stats.worker.ongoing.update': oustats,

                'stats.worker.completed.timeline': ctstats,
                'stats.worker.completed.follower': cfstats,
                'stats.worker.completed.analyzer': castats,
                'stats.worker.completed.update': custats,
            }))

    def transformJob(self, result):
        # Actually this check is pretty useless. It will fall back to None anyway
        if self.transformation in (TRANSFORM_TIMELINE, TRANSFORM_FOLLOWER, TRANSFORM_ANALYZER):
            return None

        # If you specify just -t and -a -> -f is implied
        if result.operation == result.TIMELINE_OP and \
           self.transformation & (TRANSFORM_FOLLOWER | TRANSFORM_ANALYZER):

            return False, TwitterJob(TwitterJob.FOLLOWER_OP, result.user_id, -1)

        elif result.operation == result.FOLLOWER_OP and \
             self.transformation & (TRANSFORM_ANALYZER):

            return False, TwitterJob(TwitterJob.ANALYZER_OP, result.user_id, 1)

        # TODO: please adjust this shit. Signal user process completion.
        # elif result.operation == result.ANALYZER_OP:
        #     self.redis.incr('stats.user.completed')

    def updateStatistics(self, workername, result, attributes, ot, of, oa, ou):
        """
        Update crawler statistics by sending through several
        redis channels the time series statistics

        @param workername the nickname of the worker
        @param result a TwitterJob instance result of computation (next_job)
        @param attributes attributes associated with the notification message sent
                          from the worker. It contains statistics

        @param ot number of ongoing timeline workers
        @param of number of ongoing follower workers
        @param oa number of ongoing analyzer workers
        @param ou number of ongoing update workers
        """
        now = time.time()

        if result.operation == result.TIMELINE_OP:
            total_included = attributes.get('timeline.total_included', 0)
            total_fetched = attributes.get('timeline.total_fetched', 0)

            if total_included > 0:
                self.redis.incr('timeline.total_included', total_included)
            if total_fetched > 0:
                self.redis.incr('timeline.total_fetched', total_fetched)
                self.redis.publish('stats.ops.timeline.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.FOLLOWER_OP:
            total_fetched = attributes.get('follower.total_fetched', 0)

            if total_fetched > 0:
                self.redis.incr('follower.total_fetched', total_fetched)
                self.redis.publish('stats.ops.follower.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.ANALYZER_OP:
            total_included = attributes.get('analyzer.total_included', 0)
            total_fetched = attributes.get('analyzer.total_fetched', 0)

            if total_included > 0:
                self.redis.incr('analyzer.total_included', total_included)
            if total_fetched > 0:
                self.redis.incr('analyzer.total_fetched', total_fetched)
                self.redis.publish('stats.ops.analyzer.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.UPDATE_OP:
            total_included = attributes.get('update.total_included', 0)
            total_fetched = attributes.get('update.total_fetched', 0)

            if total_included > 0:
                self.redis.incr('update.total_included', total_included)
            if total_fetched > 0:
                self.redis.incr('update.total_fetched', total_fetched)
                self.redis.publish('stats.ops.update.%s' % workername, json.dumps((total_fetched, now)))

    def notifyProgress(self, client, result, status, attributes):
        # We use this notification to check the progress of the analyzer and just add the
        # results to our stream

        ot, of, oa, ou = 0, 0, 0, 0

        if result.operation == TwitterJob.TIMELINE_OP:
            ot = self.redis.decr('stats.worker.ongoing.timeline')
            if status == STATUS_COMPLETED:
                self.redis.incr('stats.worker.completed.timeline')
        elif result.operation == TwitterJob.FOLLOWER_OP:
            of = self.redis.decr('stats.worker.ongoing.follower')
            if status == STATUS_COMPLETED:
                self.redis.incr('stats.worker.completed.follower')
        elif result.operation == TwitterJob.ANALYZER_OP:
            oa = self.redis.decr('stats.worker.ongoing.analyzer')
            if status == STATUS_COMPLETED:
                self.redis.incr('stats.worker.completed.analyzer')
        elif result.operation == TwitterJob.UPDATE_OP:
            ou = self.redis.decr('stats.worker.ongoing.update')
            if status == STATUS_COMPLETED:
                self.redis.incr('stats.worker.completed.update')

        self.updateStatistics(self.getNickname(client), result, attributes, ot + 1, of + 1, oa + 1, ou + 1)

        if result.operation == result.ANALYZER_OP:
            counter = 0

            for target_user in attributes.get('analyzer.target_users', []):
                try:
                    newjob = TwitterJob(TwitterJob.TIMELINE_OP, int(target_user), 0)

                    #log.msg("Following %d friend => %d %s" % (result.user_id, int(target_user), str(newjob)))
                    if not self.redis.sismember(settings.USERS_SELECTED, target_user):
                        self.redis.rpush(settings.FRONTIER_NAME, TwitterJob.serialize(newjob))
                        self.redis.sadd(settings.USERS_SELECTED, target_user)
                        counter += 1

                except Exception, exc:
                    log.msg("Bogus data from the analyzer: %s is not a user_id" % target_user)

            if counter > 0:
                log.msg("Adding a total of %d users. These were %d 's friends" % (counter, result.user_id))
            else:
                log.msg("No admissible friends were discovered while traversing %d 's followers list" % (result.user_id))

    def statusCompleted(self, status):
        return status == STATUS_COMPLETED

    def needsReinsertion(self, client, result, status):
        # The base class does not differentiate among several possible results
        # In this case we need to avoid unathorized requests to be put on the stream
        # again. Also we can move errors on a different stream for further analysis

        if status == STATUS_UNAUTHORIZED:
            log.msg('User id %d is protected or not found' % result.user_id)
            return False

        if status == STATUS_ERROR:
            log.msg('Error while processing user id %d. Inserting in the error queue' % result.user_id)
            self.handleFailingJob(self.getPreviousJob(client))
            return False

        return True

    def onFinished(self):
        # If we have finished and an analyzer was specified extract the frontier
        if self.transformation & (TRANSFORM_ANALYZER):
            with NamedTemporaryFile(prefix='frontier-', suffix='.gz', delete=False) as container:
                with gzip.GzipFile(mode='wb', fileobj=container) as gzdst:
                    for i in xrange(self.redis.llen(settings.FRONTIER_NAME)):
                        gzdst.write(self.redis.lindex(settings.FRONTIER_NAME, i) + '\n')

                log.msg("Frontier contents saved into %s" % container.name)

        return JobTrackerFactory.onFinished(self)


def main(options):

    connection = settings.REDIS_CLASS()

    log.startLogging(sys.stdout)
    log.startLogging(DailyLogFile.fromFullPath(os.path.join(settings.LOG_DIRECTORY, 'master.log')), setStdout=1)
    log.addObserver(RedisLogObserver(connection).emit)

    factory = TwitterJobTrackerFactory(connection, TwitterJob, settings.MAX_CLIENTS, options=options)
    reactor.listenTCP(settings.JT_PORT, factory)
    reactor.run()

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-T", action="store_true", dest="timeline",
                      help="Just download the timeline")
    parser.add_option("-F", action="store_true", dest="follower",
                      help="Just download the followers")
    parser.add_option("-A", action="store_true", dest="analyze",
                      help="Just do an analysis")
    parser.add_option("-U", action="store_true", dest="update",
                      help="Just do an update")
    parser.add_option("--seeds-file", dest="seeds_file",
                      help="Load the users from this file")
    parser.add_option("--stream-file", dest="stream_file",
                      help="Load the users from this file into the stream to be processed")
    parser.add_option("--recovery", dest="want_recovery", action="store_true", default=False,
                      help="Issue a recovery phase in case of master crash")

    (options, args) = parser.parse_args()
    main(options)