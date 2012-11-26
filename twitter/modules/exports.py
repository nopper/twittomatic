import time
from twitter import settings
from twitter.const import *
from twitter.modules import TwitterResponse
from twitter.modules.timeline import crawl_timeline
from twitter.modules.follower import crawl_followers
from twitter.modules.analyzer import analyze_followers_of
from twitter.modules.update import update_timeline

from random import randint

FACTOR = 0.001

def mock_timeline(*args, **kwargs):
    response = TwitterResponse(STATUS_COMPLETED, kwargs['user_id'], 0)
    num_requests = randint(1, 16) # At most 16 requests to download a timeline

    time.sleep(sum(map(lambda x: randint(1, 4) * FACTOR, range(num_requests))))

    total_fetched = 200 * (num_requests - 1) + randint(0, 200)
    total_included = randint(0, total_fetched)

    response['timeline.total_fetched'] = total_fetched
    response['timeline.total_included'] = total_included

    return response

def mock_update(*args, **kwargs):
    response = TwitterResponse(STATUS_COMPLETED, kwargs['user_id'], 0)
    num_requests = randint(1, 16) # At most 16 requests to download a timeline

    time.sleep(sum(map(lambda x: randint(1, 4) * FACTOR, range(num_requests))))

    total_fetched = 200 * (num_requests - 1) + randint(0, 200)
    total_included = randint(0, total_fetched)

    response['update.total_fetched'] = total_fetched
    response['update.total_included'] = total_included

    return response

def mock_followers(*args, **kwargs):
    response = TwitterResponse(STATUS_COMPLETED, kwargs['user_id'], 0)

    time.sleep(randint(0, 10))

    response['follower.total_fetched'] = randint(1, 100 * 150)

    return response

def mock_analyze(*args, **kwargs):
    response = TwitterResponse(STATUS_COMPLETED, kwargs['user_id'], 0)

    must_follow = kwargs.get('must_follow', None)

    if must_follow:
        must_follow({"screen_name": "tester", "id_str": "1234", "lang": "it", "statuses_count": 1000})

    time.sleep(randint(0, 10))

    response['analyzer.total_fetched'] = randint(1, 100)
    response['analyzer.target_users'] = ["1", "2", "3"]

    return response

# if settings.TESTING:
#crawl_timeline = mock_timeline
# update_timeline = mock_update
# crawl_followers = mock_followers
# analyze_followers_of = mock_analyze