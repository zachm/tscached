from simplejson import json

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
    for mts in MTS.from_cache(mts_key_list, redis_client):
        response_kquery = mts.build_response(kairos_time_range, response_kquery)
    response['queries'].append(response_kquery)


def warm(








