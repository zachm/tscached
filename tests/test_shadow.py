import mock
import pytest
import redis
import redlock

from testing.mock_redis import MockRedis

from tscached.shadow import become_leader
from tscached.shadow import release_leader
from tscached.shadow import process_for_readahead
from tscached.shadow import should_add_to_readahead


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
