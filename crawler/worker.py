import sys
import json
import time
import settings

from job import *
from const import *
from twisted.python import log
from twisted.internet import reactor, task
from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.error import ReactorNotRunning

def wait(seconds, result=None):
    """Returns a deferred that will be fired later"""
    d = defer.Deferred()
    reactor.callLater(seconds, d.callback, result)
    return d

class JobTrackerClient(LineReceiver):
    def __init__(self):
        self.sleep_time = 0

    def connectionMade(self):
        log.msg('Successfully connected to the JobTracker')
        self.register()
        self.requestJob()

    def deserialize(self, data):
        return json.loads(data)

    def serialize(self, data):
        return json.dumps(data)

    def lineReceived(self, line):
        msg = self.deserialize(line)

        if 'job' in msg:
            self.executeJob(self.factory.jobclass.deserialize(msg['job']))
        elif 'message' in msg:
            message = msg['message']

            if message.startswith('quit/'):
                log.msg("Stream successfully completed. Quitting")
                self.quit()

            elif message.startswith('sleep/'):
                _, interval = message.split('/', 1)

                try:
                    interval = int(interval)
                except:
                    interval = 10

                self.sleep_time = interval
                self.requestJob()

            elif message.startswith('error/'):
                reason = message.split('/', 1)[1]
                log.msg("Received error notification from the master: %s" % reason)
                reactor.stop()

    def executeJob(self, job):
        # Usually you actually run the job get the result and call the onJobCompleted callback
        # status, result, attrs = 
        raise Exception("Not implemented")

    def onJobCompleted(self, status, result, attrs):
        self.notifyMaster(status, result, attrs)
        self.requestJob()

    def notifyMaster(self, status, result, attrs):
        if result is not None:
            result = self.factory.jobclass.serialize(result)

        payload = {
            'type': 'result',
            'result': result,
            'status': status,
            'attributes': attrs
        }

        self.transport.write(self.serialize(payload) + '\r\n')

    def register(self):
        self.transport.write(self.serialize({'type': 'register', 'nickname': self.factory.nickname}) + '\r\n')

    def requestJob(self):
        if self.sleep_time > 0:
            log.msg("Sleeping %d seconds before requesting a new job" % self.sleep_time)

        d = task.deferLater(reactor, self.sleep_time,
            lambda: self.serialize({'type': 'request'}) + '\r\n'
        )
        d.addCallback(self.transport.write)

    def quit(self):
        self.factory.quit()

class JobTrackerClientFactory(ReconnectingClientFactory):
    protocol = JobTrackerClient

    def __init__(self, jobclass, nickname):
        self.jobclass = jobclass
        self.nickname = nickname
        self.stopped = False

        log.msg("Using %s as Job class" % self.jobclass.__name__)
        log.msg("Using %s as nickname" % self.nickname)

    def clientConnectionFailed(self, connector, reason):
        # TODO: if the error is an exception stop it
        log.msg('Connection with the JobTracker master failed: %s' % reason.getErrorMessage())
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        log.msg('Exception: %s' % str(reason.type))
        log.msg('Connection with the JobTracker master is lost: %s' % reason.getErrorMessage())
        self.quit()
        #ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def quit(self, proto=None):
        if not self.stopped:
            self.stopped = True
            try:
                reactor.stop()
            except ReactorNotRunning:
                log.msg("The reactor was already stopped. Probably we got a SIGINT")

def main(nickname):
    log.startLogging(sys.stdout)
    import job
    factory = JobTrackerClientFactory(job.Job, nickname)
    reactor.connectTCP(settings.JT_HOSTNAME, settings.JT_PORT, factory)
    reactor.run()

if __name__ == '__main__':
    main(sys.argv[1])
