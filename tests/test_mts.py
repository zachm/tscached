import copy
import datetime
import time
from types import GeneratorType

from freezegun import freeze_time
from mock import patch
import simplejson as json

from testing.mock_redis import MockRedis
from tscached.mts import MTS
from tscached.kquery import KQuery  # TODO remove


MTS_CARDINALITY = {
                    'tags': {'ecosystem': ['dev'], 'hostname': ['dev1']},
                    'group_by': {'name': 'tag', 'tags': ['habitat']},
                    'aggregators': {
                                    'name': 'sum',
                                    'align_sampling': True,
                                    'sampling': {'value': 10, 'unit': 'seconds'}
                                   },
                    'name': 'loadavg.05'
                  }


def test_from_result():
    """ Test from_result """
    redis_cli = MockRedis()
    results = {'results': [{'wubba-lubba': 'dub-dub'}, {'thats-the-way': 'the-news-goes'}]}
    ret_vals = MTS.from_result(results, redis_cli)
    assert isinstance(ret_vals, GeneratorType)
    ctr = 0
    for mts in ret_vals:
        assert isinstance(mts, MTS)
        assert mts.result == results['results'][ctr]
        assert mts.expiry == 10800
        assert mts.cache_type == 'mts'
        ctr += 1
    assert redis_cli.set_call_count == 0 and redis_cli.get_call_count == 0


def test_from_cache():
    redis_cli = MockRedis()
    keys = ['key1', 'key2', 'key3']
    ret_vals = list(MTS.from_cache(keys, redis_cli))
    assert redis_cli.derived_pipeline.pipe_get_call_count == 3
    assert redis_cli.derived_pipeline.execute_count == 1
    ctr = 0
    for mts in ret_vals:
        assert isinstance(mts, MTS)
        assert mts.result == {'hello': 'goodbye'}
        assert mts.expiry == 10800
        assert mts.redis_key == keys[ctr]
        ctr += 1
    assert redis_cli.set_call_count == 0 and redis_cli.get_call_count == 0


def test_key_basis():
    # simple case
    mts = MTS(MockRedis())
    mts.result = MTS_CARDINALITY
    assert mts.key_basis() == MTS_CARDINALITY

    # key_basis should remove data not explicitly included
    cardinality_with_bad_data = copy.deepcopy(MTS_CARDINALITY)
    cardinality_with_bad_data['something-irrelevant'] = 'whatever'
    mts.result = cardinality_with_bad_data
    assert mts.key_basis() == MTS_CARDINALITY

    # key_basis should not include keys that aren't set
    mts_cardinality = copy.deepcopy(MTS_CARDINALITY)
    del mts_cardinality['group_by']
    mts.result = mts_cardinality
    assert mts.key_basis() == mts_cardinality
    assert 'group_by' not in mts.key_basis().keys()


def test_upsert():
    redis_cli = MockRedis()
    mts = MTS(redis_cli)
    mts.result = MTS_CARDINALITY
    mts.redis_key = 'hello-key'
    mts.upsert()

    assert redis_cli.set_call_count == 1
    assert redis_cli.get_call_count == 0
    assert redis_cli.set_parms == [['hello-key', json.dumps(MTS_CARDINALITY), {'ex': 10800}]]


def test_merge_at_end():
    initials = [
                [1234567890000, 10], [1234567900000, 11], [1234567910000, 12], [1234567920000, 13],
                [1234567930000, 14], [1234567940000, 15], [1234567950000, 16], [1234567960000, 17],
                [1234567970000, 18], [1234567980000, 19], [1234567990000, 20]
               ]
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    # common case, data doesn't overlap
    mts.result = {'values': copy.deepcopy(initials)}
    new_mts.result = {'values': [[1234568000000, 21]]}
    mts.merge_at_end(new_mts)
    assert len(mts.result['values']) == 12
    assert mts.result['values'][0] == [1234567890000, 10]
    assert mts.result['values'][11] == [1234568000000, 21]

    # single overlapping point - make sure the new_mts version is favored
    mts.result = {'values': copy.deepcopy(initials)[:-1]}
    new_mts.result = {'values': [[1234567990000, 999], [1234568000000, 21]]}
    mts.merge_at_end(new_mts)
    assert len(mts.result['values']) == 12
    assert mts.result['values'][-2:] == [[1234567990000, 999], [1234568000000, 21]]

    # trying to overmerge with a ton of duplicate data.
    mts.result = {'values': copy.deepcopy(initials)}
    new_mts.result = {'values': copy.deepcopy(initials)}
    mts.merge_at_end(new_mts)
    assert len(mts.result['values']) == 11
    assert mts.result['values'] == initials

    mts.result = {'values': copy.deepcopy(initials)}
    new_mts.result = {'values': [[1234567980000, 19.5], [1234567990000, 20.5], [1234568010000, 25]]}
    mts.merge_at_end(new_mts)
    assert len(mts.result['values']) == 12
    assert mts.result['values'] == initials + [[1234568010000, 25]]
