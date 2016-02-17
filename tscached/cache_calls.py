import logging

import simplejson as json

from tscached.mts import MTS
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
        # mts.upsert()
        pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
        response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)

    result = pipeline.execute()
    success_count = len(filter(lambda x: x is True, result))
    logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

    kquery.upsert(start_time, end_time)  # TODO
    return response_kquery


def hot(redis_client, mts_key_list, kairos_time_range):
    # Hot / Hit
    logging.info("KQuery is HOT")
    response_kquery = {'results': [], 'sample_size': 0}
    for mts in MTS.from_cache(mts_key_list, redis_client):
        response_kquery = mts.build_response(kairos_time_range, response_kquery)
    return response_kquery


def warm(config, redis_client, kquery, kq_result, kairos_time_range, range_needed):
    # Warm / Stale
    logging.info('KQuery is WARM')

    time_dict = {
                    'start_absolute': int(range_needed[0].strftime('%s')) * 1000,
                    'end_absolute': int(range_needed[1].strftime('%s')) * 1000,
                }


    new_kairos_result = kquery.proxy_to_kairos(config['kairosdb']['host'],
                                               config['kairosdb']['port'],
                                               time_dict)

    response_kquery = {'results': [], 'sample_size': 0}

    cached_mts = {}  # redis key to MTS
    # pull in old MTS, put them in a lookup table
    for mts in MTS.from_cache(kq_result['mts_keys'], redis_client):
        kquery.add_mts(mts)  # we want to write these back eventually
        cached_mts[mts.get_key()] = mts

    # loop over newly returned MTS. if they already existed, merge/write. if not, just write.
    pipeline = redis_client.pipeline()
    for mts in MTS.from_result(new_kairos_result['queries'][0], redis_client):
        logging.debug("Size of cached_mts: %d" % len(cached_mts.keys()))

        old_mts = cached_mts.get(mts.get_key())
        if not old_mts:  # would have been added in previous loop
            kquery.add_mts(mts)
            pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
            # mts.upsert()
            response_kquery = mts.build_response(kairos_time_range, response_kquery, trim=False)
        else:
            old_mts.merge_from(mts, is_newer=True)
            pipeline.set(old_mts.get_key(), json.dumps(old_mts.result), ex=old_mts.expiry)
            # old_mts.upsert()
            response_kquery = old_mts.build_response(kairos_time_range, response_kquery)
    result = pipeline.execute()
    success_count = len(filter(lambda x: x is True, result))
    logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

    kquery.upsert(new_start_time, new_end_time)  ## TODO this is where we break.
    return response_kquery
    #response['queries'].append(response_kquery)







