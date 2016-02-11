from mock import patch
import pytest
import simplejson as json

from tscached.utils import create_key
from tscached.utils import get_timedelta
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
