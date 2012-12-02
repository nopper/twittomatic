import json
import unittest

from mock import *
from StringIO import StringIO

from twitter.const import *
from twitter.modules import fileutils, fetcher
from twitter.modules.analyzer import analyze_followers, analyze_followers_of
from twitter.modules.timeline import fetch_timeline, crawl_timeline

class TimelineModuleTest(unittest.TestCase):
    def test_fetch_timeline_empty(self):
        fetcher.fetch_url = Mock()
        fetcher.fetch_url.return_value = [None, [], MSG_OK, 0]
        assert (MSG_OK, [], 0) == fetch_timeline(user_id=22)
        fetcher.fetch_url.assert_called_with('get', 'http://api.twitter.com/1/statuses/user_timeline.json?count=200&include_rts=1&include_entities=1&user_id=22')

if __name__ == '__main__':
    unittest.main()