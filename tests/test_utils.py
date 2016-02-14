import datetime
from mock import patch
import pytest
import simplejson as json

from tscached.utils import create_key
from tscached.utils import get_timedelta
from tscached.utils import get_needed_absolute_time_range
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
    mock_post.return_value = Shim()

    assert query_kairos('localhost', 8080, {'goodbye': False})['hello'] is True
    assert mock_post.call_count == 1
    mock_post.assert_called_once_with('http://localhost:8080/api/v1/datapoints/query',
                                      data='{"goodbye": false}')


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
