import datetime
import logging

import redis
import simplejson as json

from tscached.mts import MTS
from tscached.utils import BackendQueryFailure
from tscached.utils import FETCH_AFTER
from tscached.utils import FETCH_ALL
from tscached.utils import FETCH_BEFORE
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
                                    end_cache, staleness_threshold)
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
    """ Cold / Miss """
    logging.info('KQuery is COLD')

    start_time, end_time = get_needed_absolute_time_range(kairos_time_range)

    response_kquery = {'results': [], 'sample_size': 0}
    kairos_result = kquery.proxy_to_kairos(config['kairosdb']['host'], config['kairosdb']['port'],
                                           kairos_time_range)

    pipeline = redis_client.pipeline()
    # Loop over every MTS
    for mts in MTS.from_result(kairos_result['queries'][0], redis_client):
        kquery.add_mts(mts)
        pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
        response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)

    try:
        result = pipeline.execute()
        success_count = len(filter(lambda x: x is True, result))
        logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

        kquery.upsert(start_time, end_time)  # TODO
    except redis.exceptions.RedisError as e:
        # We want to eat this redis exception, because in a catastrophe this becones a straight proxy.
        logging.error('RedisError: ' + e.message)

    return response_kquery


def hot(redis_client, kquery, kairos_time_range):
    # Hot / Hit
    logging.info("KQuery is HOT")
    response_kquery = {'results': [], 'sample_size': 0}
    for mts in MTS.from_cache(kquery.cached_data.get('mts_keys', []), redis_client):
        response_kquery = mts.build_response(kairos_time_range, response_kquery)
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

    new_start_time = datetime.datetime.fromtimestamp(float(kquery.cached_data.get('earliest_data')))
    new_end_time = datetime.datetime.fromtimestamp(float(kquery.cached_data.get('last_add_data')))

    cached_mts = {}  # redis key to MTS
    # pull in cached MTS, put them in a lookup table
    # TODO expected_resolution should be passed in
    for mts in MTS.from_cache(kquery.cached_data.get('mts_keys', []), redis_client):
        kquery.add_mts(mts)  # we want to write these back eventually
        cached_mts[mts.get_key()] = mts

    # loop over newly returned MTS. if they already existed, merge/write. if not, just write.
    pipeline = redis_client.pipeline()
    for mts in MTS.from_result(new_kairos_result['queries'][0], redis_client):

        old_mts = cached_mts.get(mts.get_key())
        if not old_mts:  # This MTS just started reporting and isn't yet in the cache (cold behavior).
            kquery.add_mts(mts)
            pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
            response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)
        else:
            if range_needed[2] == FETCH_AFTER:
                new_end_time = range_needed[1]
                old_mts.merge_at_end(mts)
            elif range_needed[2] == FETCH_BEFORE:
                new_start_time = range_needed[0]
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

        kquery.upsert(new_start_time, new_end_time)
    except redis.exceptions.RedisError as e:
        # Sneaky edge case where Redis fails after reading but before writing. Still return data!
        logging.error('RedisError: ' + e.message)
    return response_kquery
