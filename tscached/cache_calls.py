import datetime
import logging

import redis
import simplejson as json

from tscached.mts import MTS
from tscached.utils import BackendQueryFailure
from tscached.utils import FETCH_AFTER
from tscached.utils import FETCH_ALL
from tscached.utils import FETCH_BEFORE
from tscached.utils import get_chunked_time_ranges
from tscached.utils import get_range_needed
from tscached.utils import get_needed_absolute_time_range


def process_cache_hit(config, redis_client, kquery, kairos_time_range):
    """ KQuery found in cache. Decide whether to return solely cached data or to update cached data.
        If cached data should be updated, figure out how to do it.
        :param config: 'tscached' level from config file.
        :param redis_client: redis.StrictRedis
        :param kquery: kquery.KQuery object
        :param kairos_time_range: dict, time range straight from the HTTP request payload
        :return: dict, kquery response to be added to HTTP response
        :raise: utils.BackendQueryFailure, if a Kairos lookup failed.
    """
    # this relies on KQuery.get_cached() having a side effect. it must be called before this function.
    kq_result = kquery.cached_data
    try:
        start_cache = datetime.datetime.fromtimestamp(float(kq_result['earliest_data']))
        end_cache = datetime.datetime.fromtimestamp(float(kq_result['last_add_data']))
    except:  # some sort of cache malformation or error, doesn't matter what.
        start_cache = None
        end_cache = None

    start_request, end_request = get_needed_absolute_time_range(kairos_time_range)
    staleness_threshold = config['data']['staleness_threshold']

    range_needed = get_range_needed(start_request, end_request, start_cache,
                                    end_cache, staleness_threshold, kquery.window_size)
    if not range_needed:  # hot cache
        return hot(redis_client, kquery, kairos_time_range)
    else:
        merge_method = range_needed[2]
        if merge_method == FETCH_ALL:  # warm, but data doesn't support merging.
            logging.info('Odd COLD scenario: data exists.')
            return cold(config, redis_client, kquery, kairos_time_range)
        elif merge_method in [FETCH_BEFORE, FETCH_AFTER]:  # warm, merging supported.
            return warm(config, redis_client, kquery, kairos_time_range, range_needed)
        else:
            raise BackendQueryFailure("Received unsupported range_needed value: %s" % range_needed[2])


def cold(config, redis_client, kquery, kairos_time_range):
    """ Cold / Miss, with chunking.
        :param config: dict, 'tscached' level from config file.
        :param redis_client: redis.StrictRedis
        :param kquery: kquery.KQuery object
        :param kairos_time_range: dict, time range from HTTP request payload
        :return: dict, with keys sample_size (int) and results (list of dicts).
    """
    chunked_ranges = get_chunked_time_ranges(config, kairos_time_range)
    results = kquery.proxy_to_kairos_chunked(config['kairosdb']['host'], config['kairosdb']['port'],
                                             chunked_ranges)
    logging.info('KQuery is COLD - using %d chunks' % len(results))

    # Merge everything together as they come out - in chunked order - from the result.
    mts_lookup = {}
    ndx = len(results) - 1  # Results come out newest to eldest, so count backwards.
    while ndx >= 0:
        for mts in MTS.from_result(results[ndx]['queries'][0], redis_client, kquery):

            # Almost certainly a null result. Empty data should not be included in mts_lookup.
            if not mts.result or len(mts.result['values']) == 0:
                logging.debug('cache_calls.cold: got an empty chunked mts response')
                continue

            if not mts_lookup.get(mts.get_key()):
                mts_lookup[mts.get_key()] = mts
            else:
                # So, we could use merge_at_end, but it throws away beginning/ending values because of
                # partial windowing. But since we force align_start_time, we don't have that worry here.
                mts_lookup[mts.get_key()].result['values'] += mts.result['values']
        ndx -= 1

    # Accumulate the full KQuery response as the Redis operations are being queued up.
    response_kquery = {'results': [], 'sample_size': 0}
    pipeline = redis_client.pipeline()
    for mts in mts_lookup.values():
        kquery.add_mts(mts)
        pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
        logging.debug('Cold: Writing %d points to MTS: %s' % (len(mts.result['values']), mts.get_key()))
        response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)

    # Handle a fully empty set of MTS. Bail out before we upsert.
    if len(mts_lookup) == 0:
        kquery.query['values'] = []
        response_kquery['results'].append(kquery.query)
        logging.info('Received probable incorrect query; no results. Not caching!')
        return response_kquery

    # Execute the MTS Redis pipeline, then set the KQuery to its full new value.
    try:
        result = pipeline.execute()
        success_count = len(filter(lambda x: x is True, result))
        logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

        start_time = chunked_ranges[-1][0]
        end_time = chunked_ranges[0][1]
        kquery.upsert(start_time, end_time)
    except redis.exceptions.RedisError as e:
        # We want to eat this Redis exception, because in a catastrophe this becones a straight proxy.
        logging.error('RedisError: ' + e.message)

    return response_kquery


def hot(redis_client, kquery, kairos_time_range):
    """ Hot / Hit """
    logging.info("KQuery is HOT")
    response_kquery = {'results': [], 'sample_size': 0}
    for mts in MTS.from_cache(kquery.cached_data.get('mts_keys', []), redis_client):
        response_kquery = mts.build_response(kairos_time_range, response_kquery)

    # Handle a fully empty set of MTS: hand back the expected query with no values.
    if len(response_kquery['results']) == 0:
        kquery.query['values'] = []
        response_kquery['results'].append(kquery.query)
    return response_kquery


def warm(config, redis_client, kquery, kairos_time_range, range_needed):
    """ Warm / Stale
        config: nested dict loaded from the 'tscached' section of a yaml file.
        redis_client: redis.StrictRedis
        kquery: KQuery, generated from the client's request. get_cached was already called.
        kairos_time_range: dict, contents some subset of '{start,end}_{relative,absolute}'
        range_needed: describes kairos data needed to make cache complete for this request.
                      3-tuple (datetime start, datetime end, const<str>[FETCH_BEFORE, FETCH_AFTER])
    """
    logging.info('KQuery is WARM')

    expected_resolution = config['data'].get('expected_resolution', 10000)

    time_dict = {
                    'start_absolute': int(range_needed[0].strftime('%s')) * 1000 - expected_resolution,
                    'end_absolute': int(range_needed[1].strftime('%s')) * 1000,
                }

    new_kairos_result = kquery.proxy_to_kairos(config['kairosdb']['host'], config['kairosdb']['port'],
                                               time_dict)

    response_kquery = {'results': [], 'sample_size': 0}

    # Initial KQuery, and each MTS, can be slightly different on start/end. We need to get the min/max.
    start_times = [datetime.datetime.fromtimestamp(float(kquery.cached_data.get('earliest_data')))]
    end_times = [datetime.datetime.fromtimestamp(float(kquery.cached_data.get('last_add_data')))]

    cached_mts = {}  # redis key to MTS
    # pull in cached MTS, put them in a lookup table
    # TODO expected_resolution should be passed in
    for mts in MTS.from_cache(kquery.cached_data.get('mts_keys', []), redis_client):
        kquery.add_mts(mts)  # we want to write these back eventually
        cached_mts[mts.get_key()] = mts

    # loop over newly returned MTS. if they already existed, merge/write. if not, just write.
    pipeline = redis_client.pipeline()
    for mts in MTS.from_result(new_kairos_result['queries'][0], redis_client, kquery):
        old_mts = cached_mts.get(mts.get_key())

        if not old_mts:  # This MTS just started reporting and isn't yet in the cache (cold behavior).
            kquery.add_mts(mts)
            pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
            response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)
        else:
            if range_needed[2] == FETCH_AFTER:
                end_times.append(range_needed[1])
                old_mts.merge_at_end(mts)

                # This seems the only case where too-old data should be removed.
                expiry = old_mts.ttl_expire()
                if expiry:
                    start_times.append(expiry)

            elif range_needed[2] == FETCH_BEFORE:
                start_times.append(range_needed[0])
                old_mts.merge_at_beginning(mts)
            else:
                logging.error("WARM is not equipped for this range_needed attrib: %s" % range_needed[2])
                return response_kquery

            pipeline.set(old_mts.get_key(), json.dumps(old_mts.result), ex=old_mts.expiry)
            response_kquery = old_mts.build_response(kairos_time_range, response_kquery)
    try:
        result = pipeline.execute()
        success_count = len(filter(lambda x: x is True, result))
        logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

        kquery.upsert(min(start_times), max(end_times))
    except redis.exceptions.RedisError as e:
        # Sneaky edge case where Redis fails after reading but before writing. Still return data!
        logging.error('RedisError: ' + e.message)
    return response_kquery
