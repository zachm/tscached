import datetime
import logging

import simplejson as json

from tscached.mts import MTS
from tscached.utils import FETCH_AFTER
from tscached.utils import FETCH_BEFORE
from tscached.utils import get_needed_absolute_time_range


def cold(config, redis_client, kquery, kairos_time_range):
    """ Cold / Miss """
    logging.info('KQuery is COLD')

    start_time, end_time = get_needed_absolute_time_range(kairos_time_range)

    response_kquery = {'results': [], 'sample_size': 0}
    kairos_result = kquery.proxy_to_kairos(config['kairosdb']['host'],
                                           config['kairosdb']['port'],
                                           kairos_time_range)
    pipeline = redis_client.pipeline()
    # Loop over every MTS
    for mts in MTS.from_result(kairos_result['queries'][0], redis_client):
        kquery.add_mts(mts)
        pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
        response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)

    result = pipeline.execute()
    success_count = len(filter(lambda x: x is True, result))
    logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

    kquery.upsert(start_time, end_time)  # TODO
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
                    'start_absolute': int(range_needed[0].strftime('%s')) * 1000,
                    'end_absolute': int(range_needed[1].strftime('%s')) * 1000 - expected_resolution,
                }

    new_kairos_result = kquery.proxy_to_kairos(config['kairosdb']['host'],
                                               config['kairosdb']['port'],
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
        logging.debug("Size of cached_mts: %d" % len(cached_mts.keys()))

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

    result = pipeline.execute()
    success_count = len(filter(lambda x: x is True, result))
    logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

    kquery.upsert(new_start_time, new_end_time)
    return response_kquery
