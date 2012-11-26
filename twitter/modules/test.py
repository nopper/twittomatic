from pprint import pformat

from twisted.internet import reactor
import twisted.internet.defer
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

class PrinterClient(Protocol):
    def __init__(self, whenFinished):
        self.whenFinished = whenFinished

    def dataReceived(self, bytes):
        print '##### Received #####\n%s' % (bytes,)

    def connectionLost(self, reason):
        print 'Finished:', reason.getErrorMessage()
        self.whenFinished.callback(None)

def handleResponse(r):
    print "version=%s\ncode=%s\nphrase='%s'" % (r.version, r.code, r.phrase)
    for k, v in r.headers.getAllRawHeaders():
        print "%s: %s" % (k, '\n  '.join(v))
    whenFinished = twisted.internet.defer.Deferred()
    r.deliverBody(PrinterClient(whenFinished))
    return whenFinished

def handleError(reason):
    reason.printTraceback()
    reactor.stop()

def fetch_url_async(method, url, data=None, auth=None, agent=None, onCompleted=None):

def getPage(url, method):
    return Agent(reactor).request(method.upper(), url, Headers({'User-Agent': ['Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20120403211507 Firefox/14.0.1']}), None)

TIMELINE_URL = "http://api.twitter.com/1/statuses/user_timeline.json?count=200&include_rts=1&include_entities=1"


def fetch_timeline(user_id=None, screen_name=None, last_tweet_id=-1, max_requests=-1):
    """
    Download the timeline of a user if possible and return a list of tweets
    @return a tuple (msg, timeline, sleep_time)
    """

    timeline = []

    if user_id is not None:
        user_arg = "&user_id=%d" % user_id
    elif screen_name is not None:
        user_arg = "&screen_name=%s" % screen_name
    else:
        raise Exception("I need at least a user_id or a screen_name")

    count = 0
    max_id = (last_tweet_id != -1) and ('&max_id=%d' % last_tweet_id) or ''

    while True:
        url = TIMELINE_URL + user_arg + max_id

        def onHeaders(response):
            print r.headers.getAllRawHeaders()
            whenFinished = twisted.internet.defer.Deferred()
            r.deliverBody(PrinterClient(whenFinished))

        d = getPage('get', url)
        d.addCallbacks(onData, onError)

        try:
            count += 1
            r, collection, msg, sleep_time = fetcher.fetch_url('get', url)
        except fetcher.TooManyAttemptsException:
            return (MSG_BAN, timeline, 60)

        if msg == MSG_OK:
            if len(collection) == 0:
                return (msg, timeline, 0)

            timeline.extend(collection)
            max_id = '&max_id=%s' % (int(timeline[-1]['id_str']) - 1)

            url = TIMELINE_URL + user_arg + max_id

        elif msg == MSG_BAN:
            return (MSG_BAN, timeline, sleep_time)
        else:
            return (msg, timeline, sleep_time)

        if max_requests > 0 and count >= max_requests:
            return (msg, timeline, sleep_time)

if __name__ == "__main__"
    semaphore = twisted.internet.defer.DeferredSemaphore(2)

    dl = list()
    dl.append(semaphore.run(getPage, 'http://google.com'))

    dl = twisted.internet.defer.DeferredList(dl)
    dl.addCallbacks(lambda x: reactor.stop(), handleError)

    reactor.run()