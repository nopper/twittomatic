from twitter.const import *
from twitter import settings
from twitter.modules import TwitterResponse, fileutils, fetcher

FETCH_URL = settings.TWITTER_URL + "followers/ids.json?cursor={:d}&stringify_ids=true"

def fetch_followers(user_id=None, screen_name=None, cursor=-1, max_requests=-1):
    """
    Download the list of followers of a user if possible
    @return a tuple (msg, timeline, sleep_time)
    """

    count = 0
    followers = []

    if user_id is not None:
        user_arg = "&user_id=%d" % user_id
    elif screen_name is not None:
        user_arg = "&screen_name=%s" % screen_name
    else:
        raise Exception("I need at least a user_id or a screen_name")

    while True:
        url = FETCH_URL.format(cursor, user_id) + user_arg

        try:
            count += 1
            r, data, msg, sleep_time = fetcher.fetch_url('get', url)
        except fetcher.TooManyAttemptsException:
            return (MSG_BAN, followers, 60)

        if msg == MSG_OK:
            followers.extend(data['ids'])
            cursor = int(data['next_cursor_str'])
            url = FETCH_URL.format(cursor, user_id) + user_arg

            if cursor == 0 or len(data['ids']) == 0:
                return (MSG_OK, followers, 0, cursor)

        elif msg == MSG_BAN:
            return (MSG_BAN, followers, sleep_time, cursor)
        else:
            return (msg, followers, 0, cursor)

        if max_requests > 0 and count >= max_requests:
            return (msg, followers, sleep_time, cursor)

def crawl_followers(user_id, cursor=-1, must_include=lambda x: True):
    """
    Try to download the entire timeline of the use starting from a given page.
    Before starting issuing requests the last tweet_id of the user is retrieved if present.

    @return a TwitterResponse
    """

    log.msg("Retrieving followers of user_id %d" % user_id)

    # TODO: in case of duplication errors is better to open it as rw and load
    # all the users in a set thus removing duplicates
    with fileutils.open_file(user_id, 'fws', mode=fileutils.APPEND) as status:
        file, stats = status
        msg, followers, sleep_time, new_cursor = fetch_followers(user_id=user_id, cursor=cursor)

        for follower in followers:
            file.write("%s\n" % follower)

        response = TwitterResponse(TwitterResponse.msg_to_status(msg),
            user_id,
            new_cursor,
            sleep_time
        )

        response['follower.total_fetched'] = len(followers)
        stats.abort = (response.status == STATUS_ERROR or len(followers) == 0)
        return response

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-s", "--screen-name", dest="screen_name",
                      help="screen name of the target user")
    parser.add_option("-u", "--user-id", dest="user_id", type="int",
                      help="user ID of the target user")
    parser.add_option("-n", "--number", dest="number", type="int", default=1,
                      help="Limit the number of requests (default: 1)")

    (options, args) = parser.parse_args()

    if options.screen_name or options.user_id:
        msg, followers, sleep_time, _ = fetch_followers(
            screen_name=options.screen_name,
            user_id=options.user_id,
            max_requests=options.number
        )

        for follower in followers:
            print follower
    else:
        parser.print_help()
