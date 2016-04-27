import datetime

import freezegun
import mock
import pytest
import redis
import redlock

from testing.mock_redis import MockRedis
from tscached.kquery import KQuery
from tscached.shadow import become_leader
from tscached.shadow import release_leader
from tscached.shadow import perform_readahead
from tscached.shadow import process_for_readahead
from tscached.shadow import should_add_to_readahead
from tscached.utils import BackendQueryFailure


EX_CONFIG = {'shadow': {'http_header_name': 'Tscached-Shadow-Load', 'referrer_blacklist': ['edit']}}
HEADER_YES = {'Tscached-Shadow-Load': 'whatever'}
HEADER_NO = {}


def test_should_add_to_readahead_header_set():
    assert should_add_to_readahead(EX_CONFIG, 'http://whatever', HEADER_YES) is True


def test_should_add_to_readahead_edit_url_with_header():
    assert should_add_to_readahead(EX_CONFIG, 'http://grafana/blah&edit', HEADER_YES) is True


def test_should_add_to_readahead_edit_url_no_header():
    assert should_add_to_readahead(EX_CONFIG, 'http://grafana/blah&edit', HEADER_NO) is False


def test_should_add_to_readahead_sane_url_no_header():
    assert should_add_to_readahead(EX_CONFIG, 'http://grafana/blah', HEADER_NO) is True


def test_should_add_to_readahead_no_referrer():
    assert should_add_to_readahead(EX_CONFIG, None, HEADER_NO) is False


def test_should_add_to_readahead_no_referrer_yes_header():
    assert should_add_to_readahead(EX_CONFIG, None, HEADER_YES) is True


def test_process_for_readahead_yes():
    redis_cli = MockRedis()
    process_for_readahead(EX_CONFIG, redis_cli, 'tscached:kquery:WAT', 'http://wooo?edit', HEADER_YES)
    assert redis_cli.sadd_parms == [['tscached:shadow_list', 'tscached:kquery:WAT']]


def test_process_for_readahead_no():
    redis_cli = MockRedis()
    process_for_readahead(EX_CONFIG, redis_cli, 'tscached:kquery:WAT', 'http://wooo?edit', HEADER_NO)
    assert redis_cli.sadd_parms == []


@mock.patch('tscached.shadow.redlock.RedLock')
def test_become_leader_acquire_ok(m_redlock):
    m_redlock.return_value.acquire.return_value = True
    redis_cli = MockRedis()
    assert become_leader({'shadow': {}}, redis_cli) is m_redlock.return_value
    assert redis_cli.set_parms[0][0] == 'tscached:shadow_server'
    assert redis_cli.set_call_count == 1
    assert redis_cli.get_call_count == 0


@mock.patch('tscached.shadow.redlock.RedLock')
def test_become_leader_acquire_fail(m_redlock):
    m_redlock.return_value.acquire.return_value = False
    redis_cli = MockRedis()
    assert become_leader({'shadow': {}}, redis_cli) is False
    assert redis_cli.set_call_count == 0
    assert redis_cli.get_parms[0][0] == 'tscached:shadow_server'
    assert redis_cli.get_call_count == 1


@mock.patch('tscached.shadow.redlock.RedLock')
def test_become_leader_catch_rediserror(m_redlock):
    m_redlock.return_value.acquire.return_value = False
    redis_cli = MockRedis()

    def newget(_):
        raise redis.exceptions.RedisError
    redis_cli.get = newget
    assert become_leader({'shadow': {}}, redis_cli) is False


@mock.patch('tscached.shadow.redlock.RedLock')
def test_become_leader_catch_redlockerror(m_redlock):
    m_redlock.return_value.acquire.side_effect = redlock.RedLockError
    redis_cli = MockRedis()
    assert become_leader({'shadow': {}}, redis_cli) is False
    assert redis_cli.get_call_count == 0
    assert redis_cli.set_call_count == 0


@mock.patch('tscached.shadow.redlock.RedLock')
def test_become_leader_does_not_catch_other_error(m_redlock):
    m_redlock.return_value.acquire.side_effect = ValueError
    redis_cli = MockRedis()
    with pytest.raises(ValueError):
        become_leader({'shadow': {}}, redis_cli)


def test_release_leader_release_ok():
    lock = mock.Mock()
    redcli = mock.Mock()
    assert release_leader(lock, redcli) is True
    assert lock.release.call_count == 1
    redcli.delete.assert_called_once_with('tscached:shadow_server')


def test_release_leader_catch_rediserror():
    lock = mock.Mock()
    redcli = mock.Mock()
    redcli.delete.side_effect = redis.exceptions.RedisError
    assert release_leader(lock, redcli) is False
    assert lock.release.call_count == 1
    redcli.delete.assert_called_once_with('tscached:shadow_server')


def test_release_leader_catch_redlockerror():
    lock = mock.Mock()
    redcli = mock.Mock()
    lock.release.side_effect = redlock.RedLockError
    assert release_leader(lock, redcli) is False
    assert lock.release.call_count == 1
    assert redcli.delete.call_count == 0


@mock.patch('tscached.shadow.become_leader')
def test_perform_readahead_no_leader(m_become_leader):
    m_become_leader.return_value = False
    assert perform_readahead({}, MockRedis()) is None
    assert m_become_leader.call_count == 1


@freezegun.freeze_time("2016-01-01 20:00:00", tz_offset=-8)
@mock.patch('tscached.shadow.become_leader')
@mock.patch('tscached.shadow.release_leader')
@mock.patch('tscached.shadow.kquery.KQuery.from_cache')
@mock.patch('tscached.shadow.cache_calls.process_cache_hit')
def test_perform_readahead_happy_path(m_process, m_from_cache, m_release_leader, m_become_leader):
    redis_cli = MockRedis()

    def _smem(_):
        return set(['tscached:kquery:superspecial'])
    redis_cli.smembers = _smem
    m_become_leader.return_value = True
    kqueries = []
    for ndx in xrange(10):
        kq = KQuery(redis_cli)
        kq.cached_data = {'last_add_data': int(datetime.datetime.now().strftime('%s')) - 1800,
                          'redis_key': 'tscached:kquery:' + str(ndx)}
        kqueries.append(kq)
    m_from_cache.return_value = kqueries
    m_process.return_value = {'sample_size': 666}, 'warm_append'

    assert perform_readahead({}, redis_cli) is None
    assert m_become_leader.call_count == 1
    assert m_release_leader.call_count == 1
    assert m_from_cache.call_count == 1
    assert m_from_cache.call_args_list[0][0] == (['tscached:kquery:superspecial'], redis_cli)
    assert m_process.call_count == 10
    k_t_r = {'start_relative': {'unit': 'minutes', 'value': '24194605'}}
    for ndx in xrange(10):
        assert m_process.call_args_list[ndx][0] == ({}, redis_cli, kqueries[ndx], k_t_r)


@mock.patch('tscached.shadow.become_leader')
@mock.patch('tscached.shadow.release_leader')
@mock.patch('tscached.shadow.kquery.KQuery.from_cache')
@mock.patch('tscached.shadow.cache_calls.process_cache_hit')
def test_perform_readahead_redis_error(m_process, m_from_cache, m_release_leader, m_become_leader):
    redis_cli = MockRedis()

    def _smem(_):
        raise redis.exceptions.RedisError("OOPS!")
    redis_cli.smembers = _smem
    m_become_leader.return_value = True

    assert perform_readahead({}, redis_cli) is None
    assert m_become_leader.call_count == 1
    assert m_release_leader.call_count == 1
    assert m_from_cache.call_count == 0
    assert m_process.call_count == 0


@mock.patch('tscached.shadow.become_leader')
@mock.patch('tscached.shadow.release_leader')
@mock.patch('tscached.shadow.kquery.KQuery.from_cache')
@mock.patch('tscached.shadow.cache_calls.process_cache_hit')
def test_perform_readahead_backend_error(m_process, m_from_cache, m_release_leader, m_become_leader):
    redis_cli = MockRedis()

    def _smem(_):
        return set(['tscached:kquery:superspecial'])
    redis_cli.smembers = _smem
    m_become_leader.return_value = True
    kqueries = []
    for ndx in xrange(10):
        kq = KQuery(redis_cli)
        kq.cached_data = {'last_add_data': int(datetime.datetime.now().strftime('%s')) - 1800,
                          'redis_key': 'tscached:kquery:' + str(ndx)}
        kqueries.append(kq)
    m_from_cache.return_value = kqueries
    m_process.side_effect = BackendQueryFailure('OOPS!')

    assert perform_readahead({}, redis_cli) is None
    assert m_become_leader.call_count == 1
    assert m_release_leader.call_count == 1
    assert m_from_cache.call_count == 1
    assert m_process.call_count == 1
