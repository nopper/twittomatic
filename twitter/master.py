import os
import time
import settings
from crawler.master import *
from twitter.const import *
from twitter.job import TwitterJob, Stats
from twitter.modules.redislogger import RedisLogObserver

import gzip
from tempfile import NamedTemporaryFile
from collections import defaultdict

from redis import WatchError
from twisted.python import log
from twisted.python.logfile import DailyLogFile

TRANSFORM_TIMELINE = 1
TRANSFORM_FOLLOWER = 2
TRANSFORM_ANALYZER = 4
# The updater does not provide any transformation

from twitter.backend import *

class TwitterJobTrackerFactory(JobTrackerFactory):
    USERS_SEEDS    = settings.USERS_SEEDS
    USERS_SELECTED = settings.USERS_SELECTED
    FRONTIER_NAME  = settings.FRONTIER_NAME

    def __init__(self, *args, **kwargs):
        self.initialized = False
        self.options = kwargs.pop('options')

        JobTrackerFactory.__init__(self, *args, **kwargs)

        if self.options.seeds_file:
            self.loadSeedsFrom(self.options.seeds_file)

        if self.options.ha > 0:
            self.master_id = self.options.ha

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
            Stats.TIMELINE_TOTAL_INCLUDED: 0,
            Stats.TIMELINE_TOTAL_FETCHED: 0,

            Stats.FOLLOWER_TOTAL_FETCHED: 0,

            Stats.ANALYZER_TOTAL_INCLUDED: 0,
            Stats.ANALYZER_TOTAL_FETCHED: 0,

            Stats.UPDATE_TOTAL_INCLUDED: 0,
            Stats.UPDATE_TOTAL_FETCHED: 0,

            Stats.TIMELINE_ONGOING: 0,
            Stats.FOLLOWER_ONGOING: 0,
            Stats.ANALYZER_ONGOING: 0,
            Stats.UPDATE_ONGOING: 0,

            Stats.TIMELINE_COMPLETED: 0,
            Stats.FOLLOWER_COMPLETED: 0,
            Stats.ANALYZER_COMPLETED: 0,
            Stats.UPDATE_COMPLETED: 0,
        })

    def recoverFromCrash(self):
        """
        Here we need to remove all the keys from redis that are listed as assigned:
        and push back from ongoing to the stream all the works
        """
        abort = False

        with self.redis.pipeline() as pipe:
            try:
                pipe.watch(self.ONGOING % options.ha)
                pipe.watch(self.MASTER_REFCOUNT)

                log.msg("Checking if master with id %d left some works" % self.options.ha)

                assigned_keys = self.redis.keys((self.ASSIGNED % self.options.ha) + ":*")
                assigned_jobs = pipe.smembers(self.ONGOING % self.options.ha)

                if assigned_keys:
                    values = set(pipe.mget(assigned_keys))
                else:
                    values = set([])

                if values != assigned_jobs:
                    log.msg('Assigned jobs does not match the ongoing set')
                    abort = True

                if len(assigned_keys) == 0:
                    log.msg("Master with id %d is clean" % self.options.ha)
                    return

                if not self.options.want_recovery:
                    log.msg("A crash condition was detected but the --recovery flag is not enabled")
                    abort = True
                else:
                    log.msg("A crash condition was detected. Trying to recover")
                    pipe.multi()

                    for job in assigned_jobs:
                        log.msg("Reinserting %s in stream queue" % job)
                        pipe.lpush(self.STREAM, job)

                    pipe.delete(self.ONGOING % self.options.ha)
                    if assigned_keys:
                        pipe.delete(*assigned_keys)

                    counter = defaultdict(int)
                    for value in values:
                        job = self.jobclass.deserialize(job)
                        counter[job.operation] += 1

                    pipe.decr(self.MASTER_REFCOUNT)
                    pipe.decr(Stats.TIMELINE_ONGOING, counter[TwitterJob.TIMELINE_OP])
                    pipe.decr(Stats.FOLLOWER_ONGOING, counter[TwitterJob.FOLLOWER_OP])
                    pipe.decr(Stats.ANALYZER_ONGOING, counter[TwitterJob.ANALYZER_OP])
                    pipe.decr(Stats.UPDATE_ONGOING, counter[TwitterJob.UPDATE_OP])
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
        self.redis.delete(self.USERS_SEEDS)

        with open(inputfile, 'r') as input:
            for line in input:
                if line.strip():
                    try:
                        user_id = int(line.strip())
                        self.redis.sadd(self.USERS_SEEDS, user_id)
                    except:
                        pass

        log.msg("Successfully loaded %d users into USER_SEEDS" % self.redis.scard(self.USERS_SEEDS))

    def loadTargetsFrom(self, targetfile, type):
        if not targetfile:
            return

        stream_len = self.redis.llen(self.STREAM)

        if stream_len and stream_len == 0:
            raise Exception("Trying to load users into stream while the stream is non-empty")

        if type == TwitterJob.TIMELINE_OP:
            fmt = 'T,%d,0'
        elif type == TwitterJob.FOLLOWER_OP:
            fmt = 'F,%d,-1'
        elif type == TwitterJob.ANALYZER_OP:
            fmt = 'A,%d,0'
        elif type == TwitterJob.UPDATE_OP:
            fmt = 'U,%d,0'
        else:
            raise Exception("Unknown type")

        with open(targetfile, 'r') as input:
            for line in input:
                if line.strip():
                    try:
                        user_id = int(line.strip())
                        self.redis.lpush(self.STREAM, fmt % user_id)
                    except:
                        pass

        log.msg("Successfully loaded %d users into stream" % self.redis.llen(self.STREAM))

    def assignJobTo(self, client):
        type, job = JobTrackerFactory.assignJobTo(self, client)

        # Just used to keep track of some general statistics
        if type == TYPE_JOB:
            deserialized = TwitterJob.deserialize(job)

            if deserialized.operation == TwitterJob.TIMELINE_OP:
                self.redis.incr(Stats.TIMELINE_ONGOING)
            elif deserialized.operation == TwitterJob.FOLLOWER_OP:
                self.redis.incr(Stats.FOLLOWER_ONGOING)
            elif deserialized.operation == TwitterJob.ANALYZER_OP:
                self.redis.incr(Stats.ANALYZER_ONGOING)
            elif deserialized.operation == TwitterJob.UPDATE_OP:
                self.redis.incr(Stats.UPDATE_ONGOING)

        return type, job

    def manageLostClient(self, client):
        if self.clients[client] == WORKER_WORKING:
            deserialized = TwitterJob.deserialize(self.assigned_jobs[client])

            if deserialized.operation == TwitterJob.TIMELINE_OP:
                self.redis.decr(Stats.TIMELINE_ONGOING)
            elif deserialized.operation == TwitterJob.FOLLOWER_OP:
                self.redis.decr(Stats.FOLLOWER_ONGOING)
            elif deserialized.operation == TwitterJob.ANALYZER_OP:
                self.redis.decr(Stats.ANALYZER_ONGOING)
            elif deserialized.operation == TwitterJob.UPDATE_OP:
                self.redis.decr(Stats.UPDATE_ONGOING)

        return JobTrackerFactory.manageLostClient(self, client)

    def summary(self):
        if self.initialized:
            log.msg('=== STATISTICS ===')
            JobTrackerFactory.summary(self)

            otstats, ofstats, oastats, oustats, \
            ctstats, cfstats, castats, custats, \
            ftstats, ffstats, fastats, fupstats = map(lambda x: (x) and int(x) or 0,
                self.redis.mget((
                    Stats.TIMELINE_ONGOING,
                    Stats.FOLLOWER_ONGOING,
                    Stats.ANALYZER_ONGOING,
                    Stats.UPDATE_ONGOING,

                    Stats.TIMELINE_COMPLETED,
                    Stats.FOLLOWER_COMPLETED,
                    Stats.ANALYZER_COMPLETED,
                    Stats.UPDATE_COMPLETED,

                    Stats.TIMELINE_TOTAL_FETCHED,
                    Stats.FOLLOWER_TOTAL_FETCHED,
                    Stats.ANALYZER_TOTAL_FETCHED,
                    Stats.UPDATE_TOTAL_FETCHED,
                ))
            )

            log.msg("STATS: ONGOING:   Timeline: %d Follower: %d Analyzer: %d Update: %d" % \
                    (otstats, ofstats, oastats, oustats))
            log.msg("STATS: COMPLETED: Timeline: %d Follower: %d Analyzer: %d Update: %d" % \
                    (ctstats, cfstats, castats, custats))

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

            return False, TwitterJob(TwitterJob.ANALYZER_OP, result.user_id, 0)

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
            total_included = attributes.get(Stats.TIMELINE_TOTAL_INCLUDED, 0)
            total_fetched = attributes.get(Stats.TIMELINE_TOTAL_FETCHED, 0)

            if total_included > 0:
                self.redis.incr(Stats.TIMELINE_TOTAL_INCLUDED, total_included)
            if total_fetched > 0:
                self.redis.incr(Stats.TIMELINE_TOTAL_FETCHED, total_fetched)
                self.redis.publish('stats.ops.timeline.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.FOLLOWER_OP:
            total_fetched = attributes.get(Stats.FOLLOWER_TOTAL_FETCHED, 0)

            if total_fetched > 0:
                self.redis.incr(Stats.FOLLOWER_TOTAL_FETCHED, total_fetched)
                self.redis.publish('stats.ops.follower.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.ANALYZER_OP:
            total_included = attributes.get(Stats.ANALYZER_TOTAL_INCLUDED, 0)
            total_fetched = attributes.get(Stats.ANALYZER_TOTAL_FETCHED, 0)

            if total_included > 0:
                self.redis.incr(Stats.ANALYZER_TOTAL_INCLUDED, total_included)
            if total_fetched > 0:
                self.redis.incr(Stats.ANALYZER_TOTAL_FETCHED, total_fetched)
                self.redis.publish('stats.ops.analyzer.%s' % workername, json.dumps((total_fetched, now)))

        elif result.operation == result.UPDATE_OP:
            total_included = attributes.get(Stats.UPDATE_TOTAL_INCLUDED, 0)
            total_fetched = attributes.get(Stats.UPDATE_TOTAL_FETCHED, 0)

            if total_included > 0:
                self.redis.incr(Stats.UPDATE_TOTAL_INCLUDED, total_included)
            if total_fetched > 0:
                self.redis.incr(Stats.UPDATE_TOTAL_FETCHED, total_fetched)
                self.redis.publish('stats.ops.update.%s' % workername, json.dumps((total_fetched, now)))

    def notifyProgress(self, client, result, status, attributes):
        # We use this notification to check the progress of the analyzer and just add the
        # results to our stream

        ot, of, oa, ou = 0, 0, 0, 0

        if result.operation == TwitterJob.TIMELINE_OP:
            ot = self.redis.decr(Stats.TIMELINE_ONGOING)
            if status == STATUS_COMPLETED:
                self.redis.incr(Stats.TIMELINE_COMPLETED)
        elif result.operation == TwitterJob.FOLLOWER_OP:
            of = self.redis.decr(Stats.FOLLOWER_ONGOING)
            if status == STATUS_COMPLETED:
                self.redis.incr(Stats.FOLLOWER_COMPLETED)
        elif result.operation == TwitterJob.ANALYZER_OP:
            oa = self.redis.decr(Stats.ANALYZER_ONGOING)
            if status == STATUS_COMPLETED:
                self.redis.incr(Stats.ANALYZER_COMPLETED)
        elif result.operation == TwitterJob.UPDATE_OP:
            ou = self.redis.decr(Stats.UPDATE_ONGOING)
            if status == STATUS_COMPLETED:
                self.redis.incr(Stats.UPDATE_COMPLETED)

        self.updateStatistics(self.getNickname(client), result, attributes, ot + 1, of + 1, oa + 1, ou + 1)

        if result.operation == result.ANALYZER_OP:
            counter = 0

            for target_user in attributes.get('analyzer.target_users', []):
                try:
                    newjob = TwitterJob(TwitterJob.TIMELINE_OP, int(target_user), 0)

                    #log.msg("Following %d friend => %d %s" % (result.user_id, int(target_user), str(newjob)))

                    # NOTE: This is the point you may want to customize. As it is now new jobs
                    #       are simply inserted in the FRONTIER_NAME. If you are willing to implement
                    #       a continuous BFS just insert the job at the of STREAM (rpush).
                    #       For DFS traversal insert the job in front of STREAM (lpush).
                    #       If you need to implement a priority queue use ZSETs
                    if not self.redis.sismember(self.USERS_SELECTED, target_user):
                        if settings.TRAVERSING.upper() == 'BFS':
                            self.redis.rpush(self.STREAM, TwitterJob.serialize(newjob))
                        elif settings.TRAVERSING.upper() == 'DFS':
                            self.redis.lpush(self.STREAM, TwitterJob.serialize(newjob))
                        else:
                            self.redis.rpush(self.FRONTIER_NAME, TwitterJob.serialize(newjob))

                        self.redis.sadd(self.USERS_SELECTED, target_user)
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
        if self.transformation & (TRANSFORM_ANALYZER) and \
           settings.TRAVERSING.upper() not in ("BFS", "DFS"):

            with NamedTemporaryFile(prefix='frontier-', suffix='.gz', delete=False) as container:
                with gzip.GzipFile(mode='wb', fileobj=container) as gzdst:
                    for i in xrange(self.redis.llen(self.FRONTIER_NAME)):
                        gzdst.write(self.redis.lindex(self.FRONTIER_NAME, i) + '\n')

                log.msg("Frontier contents saved into %s" % container.name)

        return JobTrackerFactory.onFinished(self)


def main(options):

    connection = settings.REDIS_CLASS()

    log.startLogging(sys.stdout)
    log.startLogging(DailyLogFile.fromFullPath(os.path.join(settings.LOG_DIRECTORY, 'master.log')), setStdout=1)
    log.addObserver(RedisLogObserver(connection).emit)

    factory = TwitterJobTrackerFactory(connection, TwitterJob, settings.MAX_CLIENTS, options=options)
    reactor.listenTCP(settings.JT_PORT + options.ha, factory)
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
    parser.add_option("--ha", dest="ha", type="int", default=0,
                      help="Specify the ID of the master in high-availability (experimental)")

    (options, args) = parser.parse_args()
    main(options)
