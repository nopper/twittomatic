import json
import unittest

from mock import *
from StringIO import StringIO

from twitter.const import *
from twitter.modules import fileutils, fetcher
from twitter.modules.analyzer import FollowerReader, analyze_followers, analyze_followers_of
from twitter.modules.timeline import fetch_timeline, crawl_timeline

class FollowerReaderTest(unittest.TestCase):
    def test_simple(self):
        file = StringIO("1\n2\n3\nasd\n6\n")
        reader = FollowerReader(file, "StringIO")
        followers = [follower for follower in reader.get_followers()]
        assert followers == [(1, 1), (2, 2), (3, 3), (5, 6)]
        assert reader.follower_count == 4

def mock_open_file(contents):
    tuple_mock = Mock()

    file, stats = StringIO(contents), MagicMock(spec=dict)
    tuple_mock.return_value = (file, stats)

    mock = Mock()
    mock.__enter__ = tuple_mock
    mock.__exit__ = Mock()
    mock.__exit__.return_value = False

    fileutils.open_file = MagicMock()
    fileutils.open_file.return_value = mock

    return file, stats

class TimelineModuleTest(unittest.TestCase):
    def test_fetch_timeline_empty(self):
        fetcher.fetch_url = Mock()
        fetcher.fetch_url.return_value = [None, [], MSG_OK, 0]
        assert (MSG_OK, [], 0) == fetch_timeline(user_id=22)
        fetcher.fetch_url.assert_called_with('get', 'http://api.twitter.com/1/statuses/user_timeline.json?count=200&include_rts=1&include_entities=1&user_id=22')

    def test_crawl_timeline(self):
        file, stats = mock_open_file(
            json.dumps({'id_str': 12345}) + '\n'
        )

        fetcher.fetch_url = Mock()
        fetcher.fetch_url.return_value = [None, [], MSG_OK, 0]

        response = crawl_timeline(22)

        assert str(response) == "COMPLETED user_id: 22 state: 0 sleep_time: 0 attributes: timeline.total_fetched=0, timeline.total_included=0"
        fetcher.fetch_url.assert_called_with('get', 'http://api.twitter.com/1/statuses/user_timeline.json?count=200&include_rts=1&include_entities=1&user_id=22&max_id=12345')

    def test_crawl_timeline_abort_file(self):
        file, stats = mock_open_file(
            json.dumps({'id_str': 12345}) + '\n'
        )

        fetcher.fetch_url = Mock()
        fetcher.fetch_url.return_value = [None, [], MSG_UNK, 33]

        response = crawl_timeline(22)
        assert str(response) == "ERROR user_id: 22 state: 0 sleep_time: 33 attributes: timeline.total_fetched=0, timeline.total_included=0"
        assert stats.abort == True

class AnalyzerModuleTest(unittest.TestCase):
    def new_reader(self, contents="1\n2\n3\nfoobar\n\n"):
        file = StringIO(contents)
        return FollowerReader(file, "StringIO")

    def test_analyze_followers(self):
        fetcher.fetch_url = Mock()
        fetcher.fetch_url.return_value = [None, [{1: 2}, {2: 3}, {3: 4}], MSG_OK, 0]

        # Next cursor is 5 although the file is end. The MSG_OK must stop
        assert (MSG_OK, [{1: 2}, {2: 3}, {3: 4}], 0, 5) == analyze_followers(self.new_reader())

    def test_analyze_followers_huge(self):
        returns = [
            range(0, 100),
            range(100, 200),
            range(200, 300),
            range(300, 333)
        ]
        def side_effect(*args, **kwargs):
            #print args, kwargs
            return [None, returns.pop(0), MSG_OK, 0]

        fetcher.fetch_url = Mock(side_effect=side_effect)

        assert (0, range(333), 0, 333) == analyze_followers(self.new_reader('\n'.join(map(str, range(333)))))

    def test_analyze_followers_huge_processed(self):
        returns = [
            range(0, 100),
            range(100, 200),
            range(200, 300),
            range(300, 333)
        ]
        expected = [
            ','.join(map(str, range(1, 201, 2))),
            ','.join(map(str, range(201, 333, 2)))
        ]
        def side_effect(*args, **kwargs):
            arr = expected.pop(0)
            assert kwargs['data']['user_id'] == arr
            return [None, map(int, arr.split(',')), MSG_OK, 0]

        fetcher.fetch_url = Mock(side_effect=side_effect)

        assert (0, range(1, 333, 2), 0, 333) == analyze_followers(
            self.new_reader('\n'.join(map(str, range(333)))),
            already_processed=lambda x: (x % 2 == 0)
        )


if __name__ == '__main__':
    unittest.main()