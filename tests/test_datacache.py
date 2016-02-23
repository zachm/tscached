from mock import patch

from testing.mock_redis import MockRedis
from tscached.datacache import DataCache


@patch('tscached.datacache.create_key', autospec=True)
def test__init__(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    redis_cli = MockRedis()

    dc = DataCache(redis_cli, 'sometype')

    assert dc.redis_client == redis_cli
    assert dc.cache_type == 'sometype'


@patch('tscached.datacache.create_key', autospec=True)
def test_make_key(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    dc = DataCache(MockRedis(), 'sometype')
    dc.make_key()
    m_create_key.assert_called_once_with('{}', 'sometype')
    assert dc.redis_key == 'some-redis-key'


@patch('tscached.datacache.create_key', autospec=True)
def test_get_key(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    dc = DataCache(MockRedis(), 'sometype')
    dc.redis_key = None
    assert dc.get_key() == 'some-redis-key'
    assert m_create_key.call_count == 1


@patch('tscached.datacache.create_key', autospec=True)
def test_set_cached(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    redis_cli = MockRedis()
    dc = DataCache(redis_cli, 'sometype')
    dc.expiry = 9001
    dc.set_cached('value-to-set')
    assert redis_cli.set_call_count == 1
    assert redis_cli.set_parms == [['some-redis-key', '"value-to-set"', {'ex': 9001}]]

    redis_cli.success_flag = False
    dc.set_cached('some-other-value')
    assert redis_cli.set_call_count == 2
    assert redis_cli.set_parms[1] == ['some-redis-key', '"some-other-value"', {'ex': 9001}]


@patch('tscached.datacache.create_key', autospec=True)
def test_process_cached_data(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    dc = DataCache(MockRedis(), 'sometype')
    assert dc.process_cached_data(None) is False
    assert dc.process_cached_data(False) is False
    assert dc.process_cached_data('"value-I-got"') == 'value-I-got'


@patch('tscached.datacache.create_key', autospec=True)
def test_get_cached(m_create_key):
    m_create_key.return_value = 'some-redis-key'
    redis_cli = MockRedis()
    dc = DataCache(redis_cli, 'sometype')
    assert dc.get_cached() == {'hello': 'goodbye'}
    assert redis_cli.get_call_count == 1
    assert redis_cli.get_parms == [['some-redis-key']]
