"""
Simple module to manage updates of the timeline. Work in the opposite
direction of the timeline module and it actually exploits the fetch_timeline
of the timeline module
"""
import json
from twitter.const import *
from twitter.modules import TwitterResponse
from twitter.modules import fileutils
from twitter.modules.timeline import fetch_timeline

def update_timeline(user_id, page=0, must_include=lambda x: True):
    """
    Try to download the entire timeline of the use starting from a given page.
    Before starting issuing requests the last tweet_id of the user is retrieved if present.

    @return a TwitterResponse
    """
    # Here we just need to load the first line and get the id_str of the first
    # tweet.
    since_id = -1
    with fileutils.open_file(user_id, 'twt', mode=fileutils.READ) as status:
        input, stats = status
        
        for line in input:
            tweet = json.loads(line)
            since_id = int(tweet['id_str'])
            break

        if since_id == -1:
            raise Exception('Unable to load last tweet id for user %d' % user_id)

        msg, timeline, sleep_time = fetch_timeline(user_id, since_id=since_id)

        if len(timeline) > 0 and msg == MSG_OK:
            # Here we need to create a new file containing the delta timeline
            # and also include the previous tweets

            total_included = 0
            total_fetched = len(timeline)

            file = fileutils.new_tempfile()

            for tweet in timeline:
                if must_include(tweet):
                    file.write("%s\n" % json.dumps(tweet))
                    total_included += 1

                    print tweet['text']

            with fileutils.open_file(user_id, 'twt', mode=fileutils.READ) as status:
                input, stats = status
                fileutils.copy_contents(input, file)

            fileutils.commit_file(file, user_id, 'twt')
            file.close()

            response = TwitterResponse(TwitterResponse.msg_to_status(msg),
                user_id,
                page,
                sleep_time
            )

            response['update.total_included'] = total_included
            response['update.total_fetched'] = total_fetched

            return response

        # Well this is an unfortunate yet interesting situation which we decided
        # not to handle at all since. In this case the delta update is not 
        # completely downloaded because may be you hit the rate limit. Therefore
        # if we write the information we collected in the file we will end up
        # having a hole in the timeline.
        # The official REST API documentation specify that you can at most download
        # the latest 3200 tweets of a twitter account. Assuming you are the unluckiest
        # person in the world and you have downloaded in a previous round just one
        # tweet. The delta will be of 3199 tweets. Let's anyway round it to 3200 :D
        # You have your 150 requests. On each request you get at most 200 tweets.
        # That is 30.000 tweets.
        # But since we are very pessimistic let's find out the lower bound in the
        # number of requests to get that delta in one shot. The minimum number of
        # requests is 3200 / 200 = 16 requests
        
        if len(timeline) > 0:
            log.msg("Very unfortunate condition met during update. Rolling back")

        return TwitterResponse(TwitterResponse.msg_to_status(msg),
            user_id,
            page,
            sleep_time
        )