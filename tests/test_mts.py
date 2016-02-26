import copy
import datetime
from types import GeneratorType

import simplejson as json

from testing.mock_redis import MockRedis
from tscached.mts import MTS


INITIAL_MTS_DATA = [
                    [789, 10], [790, 11], [791, 12], [792, 13], [793, 14], [794, 15],
                    [795, 16], [796, 17], [797, 18], [798, 19], [799, 20]
                   ]

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


def test_key_basis_simple():
    """ simple case """
    mts = MTS(MockRedis())
    mts.result = MTS_CARDINALITY
    assert mts.key_basis() == MTS_CARDINALITY


def test_key_basis_removes_bad_data():
    """ should remove data not explicitly included """
    mts = MTS(MockRedis())
    cardinality_with_bad_data = copy.deepcopy(MTS_CARDINALITY)
    cardinality_with_bad_data['something-irrelevant'] = 'whatever'
    mts.result = cardinality_with_bad_data
    assert mts.key_basis() == MTS_CARDINALITY


def test_key_basis_no_unset_keys():
    """ should not include keys that aren't set """
    mts = MTS(MockRedis())
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


def test_merge_at_end_no_overlap():
    """ common case, data doesn't overlap """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': [[800, 21]]}
    mts.merge_at_end(new_mts)
    assert mts.result['values'] == INITIAL_MTS_DATA + [[800, 21]]


def test_merge_at_end_one_overlap():
    """ single overlapping point - make sure the new_mts version is favored """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': [[799, 9001], [800, 21]]}
    mts.merge_at_end(new_mts)
    assert mts.result['values'][-3:] == [[798, 19], [799, 9001], [800, 21]]


def test_merge_at_end_replaces_when_existing_data_is_short():
    """ if we can't iterate over the cached data, and it's out of order, we replace it. """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    new_mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    mts.result = {'values': [[789, 100], [790, 110]]}
    mts.merge_at_end(new_mts)
    assert mts.result['values'] == INITIAL_MTS_DATA


def test_merge_at_end_too_much_overlap():
    """ trying to merge so much duplicate data we give up and return just the cached data """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    mts.merge_at_end(new_mts)
    assert mts.result['values'] == INITIAL_MTS_DATA


def test_merge_at_beginning_no_overlap():
    """ common case, no overlap """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': [[788, 9]]}
    mts.merge_at_beginning(new_mts)
    assert mts.result['values'] == [[788, 9]] + INITIAL_MTS_DATA


def test_merge_at_beginning_two_overlap():
    """ single overlapping point - make sure the new_mts version is favored """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': [[788, 9], [789, 9001], [790, 10001]]}
    mts.merge_at_beginning(new_mts)
    assert mts.result['values'] == [[788, 9], [789, 9001], [790, 10001]] + INITIAL_MTS_DATA[2:]


def test_merge_at_beginning_replaces_when_existing_data_is_short():
    """ if we can't iterate over the cached data, and it's out of order, we replace it. """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    new_mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    mts.result = {'values': [[795, 1000], [797, 1100]]}
    mts.merge_at_beginning(new_mts)
    assert mts.result['values'] == INITIAL_MTS_DATA


def test_merge_at_beginning_too_much_overlap():
    """ trying to merge so much duplicate data we give up and return just the cached data """
    mts = MTS(MockRedis())
    mts.key_basis = lambda: 'some-key-goes-here'
    new_mts = MTS(MockRedis())

    mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    new_mts.result = {'values': copy.deepcopy(INITIAL_MTS_DATA)}
    mts.merge_at_beginning(new_mts)
    assert mts.result['values'] == INITIAL_MTS_DATA


def test_robust_trim_no_end():
    mts = MTS(MockRedis())
    data = []
    for i in xrange(1000):
        data.append([(1234567890 + i) * 1000, 0])
    mts.result = {'values': data}

    gen = mts.robust_trim(datetime.datetime.fromtimestamp(1234567990))
    assert len(list(gen)) == 900


def test_robust_trim_with_end():
    mts = MTS(MockRedis())
    data = []
    for i in xrange(1000):
        data.append([(1234567890 + i) * 1000, 0])
    mts.result = {'values': data}

    gen = mts.robust_trim(datetime.datetime.fromtimestamp(1234567990),
                          datetime.datetime.fromtimestamp(1234568290))
    assert len(list(gen)) == 301
