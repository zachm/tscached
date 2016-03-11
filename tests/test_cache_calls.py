
from freezegun import freeze_time
import mock

from tscached import cache_calls
from tscached.kquery import KQuery
from testing.mock_redis import MockRedis
from tscached.mts import MTS


@freeze_time("2016-01-01 20:00:00", tz_offset=-8)
@mock.patch('tscached.cache_calls.MTS.from_cache')
def test_hot(m_from_cache):
    redis_cli = MockRedis()

    def _fake_build_response(_b, response_kquery, _c=True):
        response_kquery['sample_size'] += 100
        response_kquery['results'].append({'hello': 'goodbye'})
        return response_kquery

    mts_list = []
    for i in xrange(3):
        mts = MTS(redis_cli)
        mts.build_response = _fake_build_response
        mts_list.append(mts)

    m_from_cache.return_value = mts_list

    kq = KQuery(redis_cli)
    kq.cached_data = {'mts_keys': ['kquery:mts:1', 'kquery:mts:2', 'kquery:mts:3']}
    kairos_time_range = {'start_relative': {'unit': 'hours', 'value': '1'}}

    out = cache_calls.hot(redis_cli, kq, kairos_time_range)
    assert out['sample_size'] == 300
    assert len(out['results']) == 3
