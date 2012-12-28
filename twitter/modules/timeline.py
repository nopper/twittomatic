from twitter.const import *
from twitter import settings
from twitter.modules import TwitterResponse
from twitter.backend import TimelineFile
from twitter.modules import fetcher
from twisted.python import log

TIMELINE_URL = settings.TWITTER_URL + "statuses/user_timeline.json?count=200&include_rts=1&include_entities=1"

def fetch_timeline(user_id=None, screen_name=None, last_tweet_id=-1, since_id=-1, max_requests=-1):
    """
    Download the timeline of a user if possible and return a list of tweets.

    Remeber to actually decrement max_id

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
    since_id = (since_id != -1) and ('&since_id=%d' % since_id) or ''

    while True:
        url = TIMELINE_URL + user_arg + max_id + since_id

        try:
            count += 1
            r, collection, msg, sleep_time = fetcher.fetch_url('get', url)
        except fetcher.TooManyAttemptsException:
            return (MSG_BAN, timeline, settings.TWITTER_TOOMANY_SLEEP)

        if msg == MSG_OK:
            if len(collection) == 0:
                return (msg, timeline, 0)

            # If there are no updates we waste 1 request

            timeline.extend(collection)
            max_id = '&max_id=%s' % (int(timeline[-1]['id_str']) - 1)

            url = TIMELINE_URL + user_arg + max_id + since_id

        elif msg == MSG_BAN:
            return (MSG_BAN, timeline, sleep_time)
        else:
            return (msg, timeline, sleep_time)

        if max_requests > 0 and count >= max_requests:
            return (msg, timeline, sleep_time)

def crawl_timeline(user_id, must_include=lambda x: True):
    """
    Try to download the entire timeline of the use starting from a given page.
    Before starting issuing requests the last tweet_id of the user is retrieved if present.

    @return a TwitterResponse
    """

    log.msg("Fetching timeline of user_id %d" % user_id)

    writer = TimelineFile(user_id)

    max_id = ''
    total_tweets = writer.get_total()

    try:
        last_tweet_id = int(writer.get_last()['id_str']) - 1
    except:
        log.msg("This seems to be a new timeline file")
        last_tweet_id = -1

    msg, timeline, sleep_time = fetch_timeline(user_id=user_id, last_tweet_id=last_tweet_id)

    total_included = 0
    total_fetched = len(timeline)
    total_tweets += total_included

    timeline = filter(must_include, timeline)
    total_included = len(timeline)

    writer.add_tweets(timeline)

    # Signal completion
    must_include(None)

    response = TwitterResponse(TwitterResponse.msg_to_status(msg),
        user_id,
        0,
        sleep_time
    )

    if total_fetched >= 2:
        screen_name = timeline[0]['user']['screen_name']
        first_tweet = timeline[0]['text'].replace('\n', '').replace('\r', '').replace('\t', '').encode('utf8')
        last_tweet = timeline[-1]['text'].replace('\n', '').replace('\r', '').replace('\t', '').encode('utf8')

        # TODO: We could add some statics like the number of hashtags and so on.
        # but may be we could exploits the pub/sub architecture. Other option is
        # to use directly the must_follow callback to collect statistics
        log.msg("Got %d tweets for user_id %d screen_name %s" % (total_fetched, user_id, screen_name))
        log.msg("  First tweet: '%s'" % first_tweet)
        log.msg("  Last tweet:  '%s'" % last_tweet)

    response['timeline.total_included'] = total_included
    response['timeline.total_fetched'] = total_fetched

    if response.status != STATUS_ERROR and total_fetched > 0:
        writer.commit()

    return response

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-s", "--screen-name", dest="screen_name",
                      help="screen name of the target user")
    parser.add_option("-u", "--user-id", dest="user_id", type="int",
                      help="user ID of the target user")
    parser.add_option("-e", "--expression", dest="expression", default="text",
                      help="print out only a specific portion of the JSON (default: text)")
    parser.add_option("-n", "--number", dest="number", type="int", default=1,
                      help="Limit the number of requests (default: 1)")

    (options, args) = parser.parse_args()

    if options.screen_name or options.user_id:
        msg, timeline, sleep_time = fetch_timeline(
            screen_name=options.screen_name,
            user_id=options.user_id,
            max_requests=options.number
        )

        for tweet in timeline:
            result = []

            for subexpr in options.expression.split(','):
                obj = tweet
                for param in subexpr.split('/'):
                    obj = obj.get(param, '')
                result.append(unicode(obj))

            print ('\t'.join(result)).encode('utf8')
    else:
        parser.print_help()
