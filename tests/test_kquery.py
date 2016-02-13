from types import GeneratorType

#from mock import patch

from testing.mock_redis import MockRedis
from tscached.kquery import KQuery


def test_init():
    assert KQuery(MockRedis()).related_mts == set()


def test_from_request():
    redis_cli = MockRedis()
    example_request = {
                       'metrics': [{'hello': 'some query'}, {'goodbye': 'another_query'}],
                       'start_relative': {'value': '1', 'unit': 'hours'}
                      }
    ret_vals = KQuery.from_request(example_request, redis_cli)
    assert isinstance(ret_vals, GeneratorType)

    ctr = 0
    for kq in ret_vals:
        assert isinstance(kq, KQuery)
        assert kq.query == example_request['metrics'][ctr]
        ctr += 1
    assert redis_cli.set_call_count == 0 and redis_cli.get_call_count == 0
