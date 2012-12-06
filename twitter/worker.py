import os
import json
from crawler.worker import *
from twitter.const import *
from twitter import settings
from twitter.job import TwitterJob
from twitter.modules import TwitterResponse, fileutils, exports, decorators

from twisted.internet import threads
from twisted.python import log, failure
from twisted.python.logfile import DailyLogFile

class TwitterTrackerClient(JobTrackerClient):
    def __init__(self):
        self.current_job = None
        JobTrackerClient.__init__(self)

    def mustFollow(self, user):
        log.msg("User %s lang: %s tweets: %d" % (user['screen_name'], user['lang'], user['statuses_count']))

        self.factory.redis.publish('extra.lookupinfos', json.dumps(user))

        interesting = user['lang'] == 'it' and \
                      user['statuses_count'] > 100

        if not interesting:
            self.factory.redis.sadd(settings.USERS_DISCARDED, user['id_str'])

        # Save for future proofing
        user_id = int(user['id_str'])
        self.factory.alreadyProcessed.cache_key(True, user_id)

        return interesting

    def monitorTweets(self, tweet):
        if tweet is not None:
            self.factory.redis.publish('events.timeline.new_tweet', json.dumps(tweet))

        # return true in any case
        return True


    def executeJob(self, job):
        # Let's try to be more explicit as possible
        lambda_fun = None
        self.current_job = job

        if job.operation == TwitterJob.TIMELINE_OP:
            lambda_fun = lambda: exports.crawl_timeline(
                user_id=job.user_id,
                must_include=self.monitorTweets,
            )

        elif job.operation == TwitterJob.FOLLOWER_OP:
            lambda_fun = lambda: exports.crawl_followers(
                user_id=job.user_id,
                cursor=job.cursor,
            )

        elif job.operation == TwitterJob.ANALYZER_OP:
            lambda_fun = lambda: exports.analyze_followers_of(
                user_id=job.user_id,
                start_cursor=job.cursor,
                already_processed=self.factory.alreadyProcessed,
                must_follow=self.mustFollow,
            )

        elif job.operation == TwitterJob.UPDATE_OP:
            lambda_fun = lambda: exports.update_timeline(
                user_id=job.user_id,
            )

        # Because lambdas are funny
        if lambda_fun is None:
            reason = failure.Failure("I don't know how to exucute job %s" % str(job), Exception)
            return self.onJobFailed(reason)

        # Let's also mark the execution so we can commit suicide easily in case
        # the connection with the master is lost
        self.factory.thread_working = True
        d = threads.deferToThread(self.wrapItUp(lambda_fun))
        d.addCallback(self.onJobReturned)
        d.addErrback(self.onJobFailed)

    def wrapItUp(self, function):
        def call_with_stats():
            with fileutils.profiled("Job execution took %s"):
                return function()

        return call_with_stats

    def onJobFailed(self, reason):
        self.factory.thread_working = False

        log.msg("Internal thread raised an exception:")
        reason.printDetailedTraceback()

        log.msg("Sending error message and exiting")
        self.notifyMaster(STATUS_ERROR, self.current_job, {})
        self.transport.loseConnection()

    def onJobReturned(self, response):
        self.factory.thread_working = False

        if not isinstance(response, TwitterResponse):
            reason = failure.Failure(Exception("I was expecting a TwitterResponse object. Got %s" % str(response)), Exception)
            return self.onJobFailed(reason)

        log.msg("Twitter job executed. Response is %s" % str(response))

        job = self.current_job
        self.current_job = None

        if response.status in (STATUS_COMPLETED, STATUS_BANNED):

            if response.status == STATUS_BANNED:
                self.sleep_time = response.sleep_time

            next_job = TwitterJob(job.operation, job.user_id, response.state)
            return self.onJobCompleted(response.status, next_job, response.attributes)

        elif response.status in (STATUS_UNAUTHORIZED, STATUS_ERROR):
            return self.onJobCompleted(response.status, job, response.attributes)

        reason = failure.Failure(Exception("Unknown status %d. Don't know how to proceed" % response.status), Exception)
        return self.onJobFailed(reason)

class TwitterTrackerClientFactory(JobTrackerClientFactory):
    protocol = TwitterTrackerClient

    def __init__(self, jobclass, nickname):
        JobTrackerClientFactory.__init__(self, jobclass, nickname)

        self.thread_working = False
        self.redis = settings.REDIS_CLASS()

    @decorators.lru_cache(maxsize=2 ** 12)
    def alreadyProcessed(self, user_id):
        return self.redis.sismember(settings.USERS_SEEDS, user_id) or \
               self.redis.sismember(settings.USERS_DISCARDED, user_id) or \
               self.redis.sismember(settings.USERS_SELECTED, user_id)

    def quit(self):
        JobTrackerClientFactory.quit(self)

        if self.thread_working and settings.FORCE_KILL_ON_DISCONNECT:
            log.msg("The background thread is still working. Committing suicide")
            os.kill(os.getpid(), 9)

def main(nickname):
    log.startLogging(sys.stdout)
    log.startLogging(DailyLogFile.fromFullPath(os.path.join(settings.LOG_DIRECTORY, nickname + '.log')), setStdout=1)
    factory = TwitterTrackerClientFactory(TwitterJob, nickname)
    reactor.connectTCP(settings.JT_HOSTNAME, settings.JT_PORT, factory)
    reactor.run()

if __name__ == '__main__':
    main(sys.argv[1])
