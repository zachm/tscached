
from testing.mock_redis import MockRedis

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
    assert redis_cli.sadd_parms == [['tscached:shadow', 'tscached:kquery:WAT']]


def test_process_for_readahead_no():
    redis_cli = MockRedis()
    process_for_readahead(EX_CONFIG, redis_cli, 'tscached:kquery:WAT', 'http://wooo?edit', HEADER_NO)
    assert redis_cli.sadd_parms == []
