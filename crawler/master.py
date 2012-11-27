import sys
import json

from job import *
from const import *
from twisted.internet import reactor, task, defer
from twisted.internet.protocol import Protocol, ServerFactory
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from optparse import OptionParser

WORKER_IDLE = 0
WORKER_WORKING = 1

class JobTrackerProtocol(LineReceiver):
    def connectionMade(self):
        self.client_ip = self.transport.getPeer()
        self.status = WORKER_IDLE

        log.msg("Worker connection from %s" % self.client_ip)

        if len(self.factory.clients) >= self.factory.clients_max:
            log.msg("Worker limit reached")
            self.client_ip = None
            self.transport.loseConnection()
        else:
            self.factory.clients[self.client_ip] = WORKER_IDLE
            self.factory.onNewWorker(self.client_ip)

    def deserialize(self, data):
        return json.loads(data)

    def serialize(self, data):
        return json.dumps(data)

    def onRequest(self, msg):
        assert self.status == WORKER_IDLE

        def callback(retval):
            if retval is not None:
                type, job = retval

                if type == TYPE_JOB:
                    self.writeMessage({
                        'job': job
                    })
                    self.status = WORKER_WORKING

                elif type == TYPE_MSG:
                    self.writeMessage({
                        'message': job
                    })
                    self.status = WORKER_IDLE

                return self.client_ip, type, job
            return None

        defer = self.factory.assignJobTo(self.client_ip)
        defer.addCallback(callback)
        defer.addCallback(self.factory.onJobAssigned)

    def onResult(self, msg):
        assert self.status == WORKER_WORKING

        self.status = WORKER_IDLE

        result = self.factory.jobclass.deserialize(msg['result'])
        status = msg['status']

        self.factory.jobProgress(self.client_ip, result, status)
        self.factory.notifyProgress(self.client_ip, result, status, msg.get('attributes', {}))

    def onRegister(self, msg):
        nickname = msg['nickname']

        if self.client_ip in self.factory.client_to_nick:
            self.writeMessage({
                'message': 'error/already registered'
            })
            self.transport.loseConnection()
            return

        if nickname in self.factory.nick_to_client:
            self.writeMessage({
                'message': 'error/nickname collision'
            })
            self.transport.loseConnection()
            return

        self.factory.client_to_nick[self.client_ip] = nickname
        self.factory.nick_to_client[nickname] = self.client_ip
        log.msg("%s successfully registered with nickname %s" % (self.client_ip, nickname))

    def lineReceived(self, data):
        try:
            msg = self.deserialize(data)
            msg_type = msg['type']

            if msg_type == 'request':
                self.onRequest(msg)
            elif msg_type == 'result':
                self.onResult(msg)
            elif msg_type == 'register':
                self.onRegister(msg)
        except Exception, exc:
            log.msg("Error while parsing message from client")
            log.err()
            self.transport.loseConnection()


    def writeMessage(self, msg):
        self.transport.write(self.serialize(msg) + '\r\n')

    def connectionLost(self, reason):
        if self.client_ip in self.factory.clients:
            defer.waitForDeferred(self.factory.manageLostClient(self.client_ip))

class JobTrackerFactory(ServerFactory):
    protocol = JobTrackerProtocol

    def __init__(self, redis, jobclass, clients_max=100):
        """
        @param redis a redis instance
        @param jobclass a class supporting the Job interface (serialize, deserialize)
        @param clients_max the maximum number of clients supported
        """

        self.clients_max = clients_max
        self.clients = {}

        self.client_to_nick = {}
        self.nick_to_client = {}
        self.assigned_jobs = {}

        self.jobclass = jobclass
        self.redis = None

    @defer.inlineCallbacks
    def bootstrap(self):
        import txredisapi as redis
        self.redis = yield redis.ConnectionPool()
        print self.redis

        log.msg("New JobTracker server started")
        log.msg("Using %s as Job class" % self.jobclass.__name__)

        self.periodic_summary = task.LoopingCall(self.summary)
        self.periodic_summary.start(5)

        servers = yield self.redis.get('master.refcount')

        if servers is not None or servers > 0:
            self.recoverFromCrash()
            yield self.redis.set('master.refcount', 0)

        num_instances = yield self.redis.incr('master.refcount')
        defer.returnValue(num_instances)

    def getNickname(self, client):
        return self.client_to_nick[client]

    def activeWorkers(self):
        count = 0
        for client, status in self.assigned_jobs.items():
            if status == WORKER_WORKING:
                count += 1
        return count

    def recoverFromCrash(self):
        log.msg("A crash condition was detected. You need to implement recoverFromCrash()")

    @defer.inlineCallbacks
    def summary(self):
        num_items = yield self.redis.llen('stream')
        num_workers = yield len(self.clients)
        active_workers = yield self.redis.scard('ongoing')

        if num_workers == 0:
            active_percentage = 'N/A'
        else:
            active_percentage = '%02d%%' % ((active_workers / (num_workers * 1.0)) * 100)

        log.msg("STATS: Number of workers: %d" % num_workers)
        log.msg("STATS: Number of active workers: %d [percentage: %s]" % (active_workers, active_percentage))
        log.msg("STATS: Number of items %d" % num_items)

    @defer.inlineCallbacks
    def finished(self):
        "@return True if we have successfully completed parsing the stream"
        items_left = yield self.redis.llen('stream')
        items_ongoing = yield self.redis.scard('ongoing')

        log.msg("Items left: %d Items ongoing: %d" % (items_left, items_ongoing))
        #log.msg("Contents: %s" % str(self.redis._db))

        finished = (items_left == 0 and items_ongoing == 0)

        if finished:
            log.msg("We have finished. Shutting down in 10 seconds")
            reactor.callLater(10, self.onFinished)

        defer.returnValue(finished)

    @defer.inlineCallbacks
    def assignJobTo(self, client):
        """
        @return a tuple of the form (TYPE_JOB, job) or (TYPE_MSG, msg)
        """
        assert self.clients[client] == WORKER_IDLE

        job = yield self.redis.lindex('stream', 0)

        if job is None:
            def finalize(finished):
                print finished
                if finished:
                    self.clients[client] = WORKER_IDLE
                    defer.returnValue((TYPE_MSG, "quit/"))
                else:
                    # Increase sleep interval until a given threshold of 5 minutes or so is reached
                    self.clients[client] = WORKER_IDLE
                    defer.returnValue((TYPE_MSG, "sleep/%d" % 1))

            d = self.finished()
            d.addCallback(finalize)
            yield d
        else:
            #START/TRANS#
            pipe = yield self.redis.multi()
            yield pipe.lpop('stream')
            yield pipe.set('assigned:%s' % client, job)
            yield pipe.sadd('ongoing', job)
            yield pipe.commit()
            #END/TRANS#

            self.assigned_jobs[client] = job
            self.clients[client] = WORKER_WORKING

            defer.returnValue((TYPE_JOB, job))

    def onNewWorker(self, client):
        pass

    @defer.inlineCallbacks
    def manageLostClient(self, client):
        if self.clients[client] == WORKER_WORKING:
            job = self.assigned_jobs[client]

            #START/TRANS#
            pipe = yield self.redis.multi()
            yield pipe.lpush('stream', job)
            yield pipe.delete('assigned:%s' % client)
            yield pipe.srem('ongoing', job)
            yield pipe.commit()
            #END/TRANS#

            log.msg("Client %s crashed. Job %s recovered" % (client, job))
        else:
            log.msg("Client %s crashed. No jobs were assigned" % client)

        del self.clients[client]

        try:
            nickname = self.client_to_nick[client]
            del self.nick_to_client[nickname]
        except:
            pass

            del self.client_to_nick[client]

    def statusCompleted(self, status):
        return status == True

    def jobProgress(self, client, result, status):
        assert self.clients[client] == WORKER_WORKING
        self.clients[client] = WORKER_IDLE

        if self.statusCompleted(status):
            defer.waitForDeferred(self.onJobCompleted(client, result, status))
        else:
            defer.waitForDeferred(self.onJobProgress(client, result, status))

    @defer.inlineCallbacks
    def onJobCompleted(self, client, result, status):
        prev_job = self.assigned_jobs[client]
        ret = self.transformJob(result)

        if ret:
            process_next, transformed_job = ret
            serialized_result = self.jobclass.serialize(transformed_job)
            log.msg("Job %s is tranformed into %s" % (prev_job, transformed_job))

        #START/TRANS#
        pipe = yield self.redis.multi()

        if ret is not None:
            if process_next:
                yield pipe.lpush('stream', serialized_result)
            else:
                yield pipe.rpush('stream', serialized_result)

        yield pipe.delete('assigned:%s' % client)
        yield pipe.srem('ongoing', prev_job)
        yield pipe.commit()
        #END/TRANS#

        del self.assigned_jobs[client]

    @defer.inlineCallbacks
    def onJobProgress(self, client, result, status):
        prev_job = self.assigned_jobs.get(client, None)
        serialized_result = self.jobclass.serialize(result)

        #START/TRANS#
        pipe = yield self.redis.multi()

        if self.needsReinsertion(client, result, status):
            log.msg("Reinserting..")
            yield pipe.lpush('stream', serialized_result)

        yield pipe.delete('assigned:%s' % client)
        yield pipe.srem('ongoing', prev_job)
        yield pipe.commit()
        #END/TRANS#

    def getPreviousJob(self, client):
        "@return the previously assigned job (serialized). None if no job were assigned"
        return self.assigned_jobs.get(client, None)

    def handleFailingJob(self, serialized_job):
        self.redis.rpush('error_stream', serialized_job)

    def needsReinsertion(self, client, result, status):
        return True

    def notifyProgress(self, client, result, status, attributes):
        """
        Called once a progress message is sent by a worker.
        @param client is the worker that is forwarding the message
        @param msg is a dict representing the JSON message sent by the worker

        The method is useful for collecting statistics.
        """
        pass

    def transformJob(self, job):
        """
        Take in input a Job instance and optionally returns a tuple in
        the form: (process_next:boolean, Job) or None.

        This is called only if the previous job that is completed.
        """
        raise Exception("Not implemented")

    def onFinished(self):
        reactor.stop()

def main():
    log.startLogging(sys.stdout)
    import job
    import fakeredis
    reactor.listenTCP(8000, JobTrackerFactory(fakeredis.FakeRedis(), job.Job))
    reactor.run()

if __name__ == "__main__":
    main()