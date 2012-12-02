import json
import time
import requests
from random import choice

from twitter.const import *
from twitter import settings
from twisted.python import log

AGENTS="""Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20120403211507 Firefox/14.0.1
Mozilla/5.0 (Windows NT 6.1; rv:12.0) Gecko/20120403211507 Firefox/12.0
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1
Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6
Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8) AppleWebKit/535.18.5 (KHTML, like Gecko) Version/5.2 Safari/535.18.5
Mozilla/5.0 (Linux; U; Android 4.1.1; en-us; Nexus S Build/JRO03E) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30
Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25
Mozilla/5.0 (iPhone; CPU iPhone OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Mobile/10A5376e
Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.2) Gecko/20100308 Ubuntu/10.04 (lucid) Firefox/3.6 GTB7.1""".splitlines()

def randomize_ua():
    headers = {}
    headers['User-Agent'] = choice(AGENTS)
    return headers

def get_sleep_time(r):
    if 'x-ratelimit-reset' in r.headers and \
       'x-ratelimit-remaining' in r.headers:
        remain = int(r.headers['x-ratelimit-remaining'])

        if remain == 0:

            reset = int(r.headers['x-ratelimit-reset'])

            #now = time.mktime(time.strptime(r.headers['date'],
            #                  '%a, %d %b %Y %H:%M:%S GMT'))
            now = time.time()
            diff = max(reset - now + 1, 4)

            # This is a temporary ban
            return (MSG_BAN, diff)

    if r.status_code in (500, 501, 502, 503):
        return (MSG_LIMIT, 10)

    # Not authorized
    if r.status_code == 401:
        return (MSG_NOTAUTH, 0)

    if r.status_code == 404:
        return (MSG_NOTFOUND, 0)

    if r.status_code == 200:
        return (MSG_OK, 0)

    return (MSG_UNK, 0)

class TooManyAttemptsException(Exception):
    pass

def fetch_url(method, url, data=None, auth=None, log_request=True):
    """
    Fetch a given twitter URL by respecting rate limits.
    @return (r, content, msg, sleep_time) where
             r is a Response object
             content is the body of the JSON response already parsed
             msg is an integer indicating the status of the response
             sleep_time is an integer valid only if msg == MSG_BAN or msg == MSG_LIMIT
    """

    attempts = 0
    session = requests.session()
    method = getattr(session, method)

    while attempts < settings.TWITTER_MAXATTEMPTS:
        try:
            attempts += 1
            timeout = round(settings.TWITTER_TIMEOUT_FACTOR ** attempts)

            if log_request:
                log.msg("Fetcher: URL: %s Timeout: %d" % (url, timeout))

            r = method(url, data=data, auth=auth, headers=randomize_ua(), timeout=timeout)
            msg, sleep_time = get_sleep_time(r)

            if msg == MSG_OK:
                content = json.loads(r.content)
            else:
                content = ''

            if msg == MSG_LIMIT:
                log.msg("Got an error from the server. Sleeping 10 seconds")
                time.sleep(sleep_time)
                continue

            return r, content, msg, sleep_time
        except Exception, exc:
            log.msg("Fetcher: ERROR: Attempt %d/%d: %s" % (attempts, settings.TWITTER_MAXATTEMPTS, str(exc)))
            continue

    raise TooManyAttemptsException("Error while fetching %s. Too many attempts %d" % (url, attempts))