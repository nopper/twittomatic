import sys
import time
from twitter.const import *
from twitter.modules.follower import fetch_followers

def fetch_all(user_id=None, screen_name=None, cursor=-1):
    total_followers = []

    while True:
        msg, followers, sleep_time, cursor = fetch_followers(
            screen_name=screen_name,
            user_id=user_id,
            cursor=cursor,
        )

        for follower in followers:
            total_followers.insert(0, follower)

        if sleep_time > 0:
            print >> sys.stderr, "Sleeping %s seconds before next request" % sleep_time
            time.sleep(sleep_time)

        if msg == MSG_OK:
            break

    print >> sys.stderr, "Downloaded %d followers" % len(total_followers)
    return total_followers

def get_new_additions(total_followers, user_id=None, screen_name=None, cursor=-1):
    num_request = 0
    new_additions = []
    print "Checking for new additions"

    while True:
        num_request += 1
        print "Cursor: %d Request: %d" % (num_request, cursor)

        msg, followers, sleep_time, cursor = fetch_followers(
            screen_name=screen_name,
            user_id=user_id,
            cursor=cursor,
            max_requests=1
        )

        followers = map(int, followers)

        if msg == MSG_OK:
            must_return = False

            for follower in followers:
                if follower not in total_followers:
                    new_additions.insert(0, follower)
                else:
                    must_return = True

            if must_return:
                print "%d newly added users discovered in %d requests" % (len(new_additions), num_request)
                return new_additions

        if sleep_time > 0:
            print "Sleeping %s seconds before next request" % sleep_time
            time.sleep(sleep_time)

def fetch(filename, user_id=None, screen_name=None, cursor=-1):
    with open(filename, 'a') as outputfile:
        for follower in fetch_all(user_id=user_id, screen_name=screen_name, cursor=cursor):
            outputfile.write("%s\n" % follower)

def monitor(filename, statsfile, user_id=None, screen_name=None, cursor=-1):
    followers = set()

    with open(filename, 'r') as inputfile:
        for line in inputfile:
            followers.add(int(line.strip()))

    print "Loaded %d followers from file %s" % (len(followers), filename)

    with open(filename, 'a') as outputfile:
        with open(statsfile, 'a') as stats:
            while True:
                new_additions = get_new_additions(followers, user_id=user_id, screen_name=screen_name, cursor=cursor)

                for follower in new_additions:
                    outputfile.write("%s\n" % follower)
                    followers.add(follower)

                stats.write("%d\t%d\n" % (time.time(), len(followers)))

                outputfile.flush()
                stats.flush()

                time.sleep(5 * 60) # Every five minute

if __name__ == "__main__":
    from optparse import OptionParser

    parser = OptionParser()
    parser.add_option("-s", "--screen-name", dest="screen_name",
                      help="screen name of the target user")
    parser.add_option("-u", "--user-id", dest="user_id", type="int",
                      help="user ID of the target user")
    parser.add_option("-f", "--file", dest="file",
                      help="File where to store the followers")
    parser.add_option("-t", "--stats", dest="stats",
                      help="File where to store the statistics")

    (options, args) = parser.parse_args()

    if options.file and not options.stats:
        fetch(options.file, 
              user_id=options.user_id,
              screen_name=options.screen_name)
    elif options.file and options.stats:
        monitor(options.file, options.stats,
                user_id=options.user_id,
                screen_name=options.screen_name)
    else:
        parser.print_help()
