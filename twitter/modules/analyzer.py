from twitter import settings
from twitter.const import *
from twitter.modules import TwitterResponse, fetcher
from twitter.backend import FollowerFile
from twisted.python import log

LOOKUP_URL = settings.TWITTER_URL + "users/lookup.json"

def analyze_followers(reader, start_cursor="0", already_processed=lambda x: False, progress_cb=lambda *args: None, max_requests=-1):
    """
    Analyze a list of followers contained in a given file.
    @param reader is an instance of FollowerReader
    @param already_processed is a function that takes in input an user_id and
           returns True in case the user is going to be processed or it is
           already processed.
    """

    count = 0
    batch = []
    lookup_infos = []
    next_cursor = start_cursor
    current_cursor = start_cursor
    dedup = set()

    iterable = reader.followers(start_cursor)
    number_followers = len(reader)

    while True:
        consumed = False

        while len(batch) < BATCH_LIMIT:
            try:
                follower_id, next_cursor = iterable.next()
            except StopIteration:
                consumed = True
                break

            if follower_id not in dedup and not already_processed(follower_id):
                batch.append(follower_id)
                dedup.add(follower_id)

        users = ','.join(map(str, batch))
        payload = {
            'include_entities': 'f',
            'user_id': users,
        }

        # Avoid empty request
        if len(batch) == 0:
            msg = MSG_OK
            consumed = True
            collection = []
        else:
            try:
                count += 1
                r, collection, msg, sleep_time = fetcher.fetch_url('post', LOOKUP_URL, data=payload, log_request=False)
            except fetcher.TooManyAttemptsException:
                return (MSG_BAN, lookup_infos, settings.TWITTER_TOOMANY_SLEEP, current_cursor)

        if msg == MSG_OK:
            lookup_infos.extend(collection)
            current_cursor = next_cursor

            if len(batch) > 0:
                # The +1 is actually included in the current_cursor = next_cursor assignment
                progress_cb(lookup_infos, reader.get_processed(current_cursor), number_followers)

            batch = []
            # Jump below
        else:
            return (msg, lookup_infos, sleep_time, current_cursor)

        if max_requests > 0 and count >= max_requests:
            return (msg, lookup_infos, sleep_time, current_cursor)

        if consumed:
            return (msg, lookup_infos, sleep_time, current_cursor)

        batch = []

def analyze_followers_of(user_id, start_cursor=0,
                         already_processed=lambda x: False,
                         must_follow=lambda x: True):

    log.msg("Analyzing followers of user_id %d" % user_id)

    reader = FollowerFile(user_id)

    if len(reader) == 0:
        log.msg("Follower file for user_id %d is not present. Bogus data?" % user_id)

        # Let's treat this as not found user
        return TwitterResponse(STATUS_UNAUTHORIZED, user_id, start_cursor, 0)

    def log_progress(lookup_infos, current, total):
        log.msg("user_id %d Follower file: analyzed %d of %d [%02d%%]" % \
                (user_id, current, total,
                 100 * (current / float(total))))

    msg, lookup_infos, sleep_time, current_line = analyze_followers(
        reader, start_cursor=start_cursor,
        already_processed=already_processed,
        progress_cb=log_progress
    )

    included = []

    for info in lookup_infos:
        if must_follow(info):
            included.append(info['id_str'])

    total_included = len(included)
    total_fetched = len(lookup_infos)

    response = TwitterResponse(TwitterResponse.msg_to_status(msg),
        user_id,
        current_line,
        sleep_time
    )

    response['analyzer.total_included'] = total_included
    response['analyzer.total_fetched'] = total_fetched
    response['analyzer.target_users'] = included

    return response


if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-n", "--number", dest="number", type="int", default=1,
                      help="Limit the number of requests (default: 1)")
    parser.add_option("-f", "--file", dest="filename",
                      help="followers file of the target user", metavar="FILE")
    parser.add_option("-e", "--expression", dest="expression", default="id_str,screen_name",
                      help="print out only a specific portion of the JSON (default: id_str,screen_name)")

    (options, args) = parser.parse_args()

    if options.filename:
        with open(options.filename, 'r') as infile:
            followers = infile.readlines()
            msg, lookup_infos, sleep_time, _ = analyze_followers(
                followers,
                max_requests=options.number
            )

        for info in sorted(lookup_infos):
            if options.expression:
                result = []

                for subexpr in options.expression.split(','):
                    obj = info
                    for param in subexpr.split('/'):
                        obj = obj.get(param, '')
                    result.append(unicode(obj))

                print ('\t'.join(result)).encode('utf8')
            else:
                print info
    else:
        parser.print_help()
