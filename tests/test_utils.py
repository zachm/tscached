import datetime
from mock import patch
import pytest
import simplejson as json

from freezegun import freeze_time
import requests

from tscached.utils import BackendQueryFailure
from tscached.utils import FETCH_AFTER
from tscached.utils import FETCH_ALL
from tscached.utils import FETCH_BEFORE
from tscached.utils import create_key
from tscached.utils import get_timedelta
from tscached.utils import get_chunked_time_ranges
from tscached.utils import get_needed_absolute_time_range
from tscached.utils import get_range_needed
from tscached.utils import populate_time_range
from tscached.utils import query_kairos


def test_get_timedelta():
    assert get_timedelta({'value': '157', 'unit': 'seconds'}).total_seconds() == 157
    assert get_timedelta({'value': '3', 'unit': 'minutes'}).total_seconds() == 180
    assert get_timedelta({'value': '2', 'unit': 'hours'}).total_seconds() == 7200
    assert get_timedelta({'value': '4', 'unit': 'days'}).total_seconds() == 345600
    assert get_timedelta({'value': '1', 'unit': 'weeks'}).total_seconds() == 604800

    # These reinforce behavior. Not saying that behavior is correct.
    assert get_timedelta({'value': '1', 'unit': 'months'}).total_seconds() == 2678400
    assert get_timedelta({'value': '1', 'unit': 'years'}).total_seconds() == 31536000


@patch('tscached.utils.requests.post', autospec=True)
def test_query_kairos(mock_post):
    class Shim(object):
        text = '{"hello": true}'
        status_code = 200
    mock_post.return_value = Shim()

    assert query_kairos('localhost', 8080, {'goodbye': False}) == {'hello': True}
    assert mock_post.call_count == 1
    mock_post.assert_called_once_with('http://localhost:8080/api/v1/datapoints/query',
                                      data='{"goodbye": false}')


@patch('tscached.utils.requests.post', autospec=True)
def test_query_kairos_backend_gives_non_200(mock_post):
    class Shim(object):
        text = '{"errors": ["whatever", "lol"]}'
        status_code = 500
    mock_post.return_value = Shim()
    with pytest.raises(BackendQueryFailure):
        query_kairos('localhost', 8080, {'goodbye': False})
    assert mock_post.call_count == 1


@patch('tscached.utils.requests.post', autospec=True)
def test_query_kairos_backend_gives_non_200_no_propagate(mock_post):
    class Shim(object):
        text = '{"errors": ["whatever", "lol"]}'
        status_code = 500
    mock_post.return_value = Shim()
    result = query_kairos('localhost', 8080, {'goodbye': False}, propagate=False)
    assert result['status_code'] == 500
    assert result['error'] == 'whatever, lol'
    assert mock_post.call_count == 1


@patch('tscached.utils.requests.post', autospec=True)
def test_query_kairos_backend_fails(mock_post):
    mock_post.side_effect = requests.exceptions.RequestException
    with pytest.raises(BackendQueryFailure):
        query_kairos('localhost', 8080, {'goodbye': False})
    assert mock_post.call_count == 1


@patch('tscached.utils.requests.post', autospec=True)
def test_query_kairos_backend_fails_no_propagate(mock_post):
    mock_post.side_effect = requests.exceptions.RequestException
    result = query_kairos('localhost', 8080, {'goodbye': False}, propagate=False)
    assert mock_post.call_count == 1
    assert result['status_code'] == 500
    assert result['error'] == ''


def test_create_key():
    with pytest.raises(TypeError):
        create_key(['un', 'hashable'], 'this should raise')

    assert create_key('get schwifty', 'ztm') == 'tscached:ztm:6fed3992d23c711d8c21d354f6dc46e9'
    assert create_key(json.dumps({}), 'lulz') == 'tscached:lulz:99914b932bd37a50b983c5e7c90ae93b'


def test_populate_time_range_everything():
    """ Test that needed elements from HTTP request make it in, and nothing else. """

    # Note: we do not sanity check the user's data. That's on them.
    example_request = {
                        'metrics': [],
                        'start_relative': {'value': '1', 'unit': 'hours'},
                        'something_else': 'whatever',
                        'end_relative': {'value': '1', 'unit': 'minutes'},
                        'start_absolute': 1234567890,
                        'end_absolute': 2345678901,
                      }
    time_range = populate_time_range(example_request)
    assert len(time_range.keys()) == 4
    assert time_range['start_relative'] == {'value': '1', 'unit': 'hours'}
    assert time_range['end_relative'] == {'value': '1', 'unit': 'minutes'}
    assert time_range['start_absolute'] == 1234567890
    assert time_range['end_absolute'] == 2345678901


def test_populate_time_range_subset():
    """ Test that we don't create entries that weren't set by the user. """
    example_request = {
                        'metrics': [],
                        'start_relative': {'value': '1', 'unit': 'hours'},
                      }
    assert populate_time_range(example_request) == {'start_relative': {'value': '1', 'unit': 'hours'}}


@patch('tscached.utils.datetime.datetime', autospec=True)
def test_get_needed_absolute_time_range(m_dt):
    m_dt.now.return_value = datetime.datetime.fromtimestamp(1455390419)

    # Magic: http://www.voidspace.org.uk/python/mock/examples.html#partial-mocking
    m_dt.side_effect = lambda *args, **kw: datetime.datetime(*args, **kw)

    example = {'start_absolute': 1234567890000}
    s, e = get_needed_absolute_time_range(example)
    assert e is None
    assert s == datetime.datetime.fromtimestamp(1234567890)

    example = {'start_absolute': 1234567890000, 'end_absolute': 1234657890000}
    s, e = get_needed_absolute_time_range(example)
    assert s == datetime.datetime.fromtimestamp(1234567890)
    assert e == datetime.datetime.fromtimestamp(1234657890)

    example = {
               'start_relative': {'value': '1', 'unit': 'hours'},
               'end_relative': {'value': '1', 'unit': 'minutes'}
              }
    s, e = get_needed_absolute_time_range(example)
    assert s == datetime.datetime.now() - datetime.timedelta(hours=1)
    assert e == datetime.datetime.now() - datetime.timedelta(minutes=1)


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
def test_get_chunked_time_ranges_last_15m():
    kairos_timing = {'start_relative': {'unit': 'minutes', 'value': '15'}}
    now = datetime.datetime.now()
    then = now - datetime.timedelta(minutes=15)
    assert get_chunked_time_ranges({'chunking': {}}, kairos_timing) == [(then, now)]


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
def test_get_chunked_time_ranges_last_2h():
    kairos_timing = {'start_relative': {'unit': 'hours', 'value': '2'}}
    now = datetime.datetime.now()
    results = get_chunked_time_ranges({'chunking': {'chunk_length': 1800}}, kairos_timing)
    assert len(results) == 4
    for i in xrange(len(results)):
        offset = datetime.timedelta(minutes=(i * 30))
        assert results[i][1] == now - offset
        assert results[i][0] == now - offset - datetime.timedelta(minutes=30, seconds=-1)


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
def test_get_chunked_time_ranges_last_12h():
    kairos_timing = {'start_relative': {'unit': 'hours', 'value': '12'}}
    now = datetime.datetime.now()
    results = get_chunked_time_ranges({'chunking': {'chunk_length': 1800}}, kairos_timing)
    assert len(results) == 6
    for i in xrange(len(results)):
        offset = datetime.timedelta(minutes=(i * 120))
        assert results[i][1] == now - offset
        assert results[i][0] == now - offset - datetime.timedelta(minutes=120, seconds=-1)


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
def test_get_chunked_time_ranges_last_2h15m():
    kairos_timing = {'start_relative': {'unit': 'minutes', 'value': '135'}}
    now = datetime.datetime.now()
    results = get_chunked_time_ranges({'chunking': {'chunk_length': 1800}}, kairos_timing)
    assert len(results) == 5
    for i in xrange(len(results) - 1):
        offset = datetime.timedelta(minutes=(i * 30))
        assert results[i][1] == now - offset
        assert results[i][0] == now - offset - datetime.timedelta(minutes=30, seconds=-1)
    assert results[4][0] == now - datetime.timedelta(minutes=135, seconds=-1)
    assert results[4][1] == now - datetime.timedelta(minutes=120)


def test_get_chunked_time_ranges_last_1h_clock_drift():
    """ Most tests have time frozen, so the clock doesn't drift during execution.
        This one purposely does not. It could flake if it takes over 2s to execute.
    """
    kairos_timing = {'start_relative': {'unit': 'hours', 'value': '1'}}
    now = datetime.datetime.now()
    results = get_chunked_time_ranges({'chunking': {'chunk_length': 1800}}, kairos_timing)
    assert len(results) == 2
    for i in xrange(len(results) - 1):
        offset = datetime.timedelta(minutes=(i * 30))
        end_diff = results[i][1] - (now - offset)
        assert end_diff < datetime.timedelta(seconds=2)
        begin_diff = results[i][0] - (now - offset - datetime.timedelta(minutes=30))
        assert begin_diff < datetime.timedelta(seconds=2)


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
def test_get_range_needed():

    now = datetime.datetime.now()

    def past(mins):
        return now - datetime.timedelta(minutes=mins)

    # Test broken cache data, and cache miss.
    assert get_range_needed(past(60), None, None, now) == (past(60), now, FETCH_ALL)

    # Test for hot cache: Cache has [-60, -5], Request wants [-30, -10].
    assert get_range_needed(past(30), past(10), past(60), past(5)) is False

    # Test for "stale-but-not-stale-enough": with 2 min expiry: cache [-60, -1]; request [-60, 0].
    assert get_range_needed(past(60), None, past(60), past(1), 120) is False

    # Test for too-stale (append): same test as above, but 30 second threshold.
    assert get_range_needed(past(60), None, past(60), past(1), 30) == (past(1), now, FETCH_AFTER)

    # Test for new-enough but missing old data (prepend): cache has 30m, user wants 2h.
    assert get_range_needed(past(120), now, past(30), now, 10) == (past(120), past(30), FETCH_BEFORE)

    # Test for data in middle, but missing beginning *and* end: cached [-20, -10], request [-30, 0].
    assert get_range_needed(past(30), None, past(20), past(10)) == (past(30), now, FETCH_ALL)
