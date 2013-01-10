import sys
import json

from job import *
from const import *
from twisted.internet import reactor, task
from twisted.internet.protocol import Protocol, ServerFactory
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from optparse import OptionParser

WORKER_IDLE = 0
WORKER_WORKING = 1

class JobTrackerProtocol(LineReceiver):
    MAX_LENGTH = 1 << 20

    def connectionMade(self):
        self.client_id = self.factory.mkhash(self.transport.getPeer())
        self.status = WORKER_IDLE

        log.msg("Worker connection from %s" % self.transport.getPeer())

        if len(self.factory.clients) >= self.factory.clients_max:
            log.msg("Worker limit reached")
            self.client_id = None
            self.transport.loseConnection()
        else:
            self.factory.clients[self.client_id] = WORKER_IDLE
            self.factory.onNewWorker(self.client_id)

    def deserialize(self, data):
        return json.loads(data)

    def serialize(self, data):
        return json.dumps(data)

    def onRequest(self, msg):
        assert self.status == WORKER_IDLE

        type, job = self.factory.assignJobTo(self.client_id)

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

    def onResult(self, msg):
        assert self.status == WORKER_WORKING

        self.status = WORKER_IDLE

        result = self.factory.jobclass.deserialize(msg['result'])
        status = msg['status']

        self.factory.jobProgress(self.client_id, result, status)
        self.factory.notifyProgress(self.client_id, result, status, msg.get('attributes', {}))

    def onRegister(self, msg):
        nickname = msg['nickname']

        if self.client_id in self.factory.client_to_nick:
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

        self.factory.client_to_nick[self.client_id] = nickname
        self.factory.nick_to_client[nickname] = self.client_id
        log.msg("%s successfully registered with nickname %s ID: %s" % (self.transport.getPeer(), nickname, self.client_id))

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
        if self.client_id in self.factory.clients:
            self.factory.manageLostClient(self.client_id)

class JobTrackerFactory(ServerFactory):
    protocol = JobTrackerProtocol

    MASTER_REFCOUNT = 'master.refcount'
    STREAM = 'stream'
    ERROR_STREAM = 'error_stream'
    ONGOING = 'ongoing:%d'
    ASSIGNED = 'assigned:%s'


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

        self.redis = redis
        self.jobclass = jobclass
        self.master_id = 0

        self.assigned_jobs = {}

        log.msg("New JobTracker server started")
        log.msg("Using %s as Job class" % self.jobclass.__name__)

        self.periodic_summary = task.LoopingCall(self.summary)
        self.periodic_summary.start(5)

        servers = self.redis.get(self.MASTER_REFCOUNT)

        if servers is not None or servers > 0:
            self.recoverFromCrash()
            self.redis.set(self.MASTER_REFCOUNT, 0)

        self.redis.incr(self.MASTER_REFCOUNT)

    def mkhash(self, client_ip):
        return '%d:%s:%d' % (self.master_id, client_ip.host, client_ip.port)

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

    def summary(self):
        num_items = self.redis.llen(self.STREAM)
        num_workers = len(self.clients)
        active_workers = self.redis.scard(self.ONGOING % self.master_id)

        if num_workers == 0:
            active_percentage = 'N/A'
        else:
            active_percentage = '%02d%%' % ((active_workers / (num_workers * 1.0)) * 100)

        log.msg("STATS: Number of workers: %d" % num_workers)
        log.msg("STATS: Number of active workers: %d [percentage: %s]" % (active_workers, active_percentage))
        log.msg("STATS: Number of items %d" % num_items)

    def finished(self):
        "@return True if we have successfully completed parsing the stream"
        items_left = self.redis.llen(self.STREAM)
        items_ongoing = self.redis.scard(self.ONGOING % self.master_id)

        log.msg("Items left: %d Items ongoing: %d" % (items_left, items_ongoing))
        #log.msg("Contents: %s" % str(self.redis._db))

        finished = (items_left == 0 and items_ongoing == 0)

        if finished:
            log.msg("We have finished. Shutting down in 10 seconds")
            reactor.callLater(10, self.onFinished)

        return finished

    def assignJobTo(self, client):
        """
        @return a tuple of the form (TYPE_JOB, job) or (TYPE_MSG, msg)
        """
        assert self.clients[client] == WORKER_IDLE

        job = self.redis.lindex(self.STREAM, 0)

        if job is None:
            if self.finished():
                self.clients[client] = WORKER_IDLE
                return (TYPE_MSG, "quit/")
            else:
                # Increase sleep interval until a given threshold of 5 minutes or so is reached
                self.clients[client] = WORKER_IDLE
                return (TYPE_MSG, "sleep/%d" % 60)
        else:
            #START/TRANS#
            pipe = self.redis.pipeline()
            pipe.multi()
            pipe.lpop(self.STREAM)
            pipe.set(self.ASSIGNED % client, job)
            pipe.sadd(self.ONGOING % self.master_id, job)
            pipe.execute()
            #END/TRANS#

            self.assigned_jobs[client] = job
            self.clients[client] = WORKER_WORKING

            return (TYPE_JOB, job)

    def onNewWorker(self, client):
        pass

    def manageLostClient(self, client):
        if self.clients[client] == WORKER_WORKING:
            job = self.assigned_jobs[client]

            #START/TRANS#
            pipe = self.redis.pipeline()
            pipe.multi()
            pipe.lpush(self.STREAM, job)
            pipe.delete(self.ASSIGNED % client)
            pipe.srem(self.ONGOING % self.master_id, job)
            pipe.execute()
            #END/TRANS#

            log.msg("Client %s crashed. Job %s recovered" % (client, job))
        else:
            log.msg("Client %s crashed. No jobs were assigned" % client)

        del self.clients[client]

        try:
            nickname = self.client_to_nick[client]
            del self.nick_to_client[nickname]
            del self.client_to_nick[client]
        except:
            pass

    def statusCompleted(self, status):
        return status == True

    def jobProgress(self, client, result, status):
        assert self.clients[client] == WORKER_WORKING
        self.clients[client] = WORKER_IDLE

        if self.statusCompleted(status):
            self.onJobCompleted(client, result, status)
        else:
            self.onJobProgress(client, result, status)

    def onJobCompleted(self, client, result, status):
        prev_job = self.assigned_jobs[client]
        ret = self.transformJob(result)

        if ret:
            process_next, transformed_job = ret
            serialized_result = self.jobclass.serialize(transformed_job)
            log.msg("Job %s is tranformed into %s" % (prev_job, transformed_job))

        #START/TRANS#
        pipe = self.redis.pipeline()
        pipe.multi()

        if ret is not None:
            if process_next:
                pipe.lpush(self.STREAM, serialized_result)
            else:
                pipe.rpush(self.STREAM, serialized_result)

        pipe.delete(self.ASSIGNED % client)
        pipe.srem(self.ONGOING % self.master_id, prev_job)
        pipe.execute()
        #END/TRANS#

        del self.assigned_jobs[client]

    def onJobProgress(self, client, result, status):
        prev_job = self.assigned_jobs.get(client, None)
        serialized_result = self.jobclass.serialize(result)

        #START/TRANS#
        pipe = self.redis.pipeline()
        pipe.multi()

        if self.needsReinsertion(client, result, status):
            pipe.lpush(self.STREAM, serialized_result)

        pipe.delete(self.ASSIGNED % client)
        pipe.srem(self.ONGOING % self.master_id, prev_job)
        pipe.execute()
        #END/TRANS#

    def getPreviousJob(self, client):
        "@return the previously assigned job (serialized). None if no job were assigned"
        return self.assigned_jobs.get(client, None)

    def handleFailingJob(self, serialized_job):
        self.redis.rpush(self.ERROR_STREAM, serialized_job)

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