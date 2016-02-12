import logging
import simplejson as json
import yaml

from flask import request
import redis

from tscached import app
from tscached.kquery import KQuery
from tscached.mts import MTS


with open('tscached.yaml', 'r') as config_file:
    CONF_DICT = yaml.load(config_file.read())['tscached']


if not app.debug:
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)


@app.route('/', methods=['GET'])
def handle_root():
    return "hello world!"


@app.route('/api/v1/datapoints/query', methods=['POST', 'GET'])
def handle_query():
    if request.method == 'POST':
        payload = json.loads(request.data)  # dict
    else:
        payload = json.loads(request.args.get('query'))

    logging.info('Query')
    redis_client = redis.StrictRedis(host=CONF_DICT['redis']['host'], port=CONF_DICT['redis']['port'])
    response = {'queries': []}

    # HTTP request may contain one or more kqueries
    for kquery in KQuery.from_request(payload, redis_client):
        kq_result = kquery.get_cached()
        response_kquery = {'results': [], 'sample_size': 0}
        if not kq_result:
            # Cold / Miss
            logging.info('KQuery is COLD')

            kairos_result = kquery.proxy_to_kairos(CONF_DICT['kairosdb']['host'],
                                                   CONF_DICT['kairosdb']['port'])
            pipeline = redis_client.pipeline()
            # Loop over every MTS
            for mts in MTS.from_result(kairos_result['queries'][0], redis_client):
                kquery.add_mts(mts)
                # mts.upsert()
                pipeline.set(mts.get_key(), json.dumps(mts.result), ex=mts.expiry)
                response_kquery = mts.build_response(kquery, response_kquery, trim=False)

            result = pipeline.execute()
            success_count = len(filter(lambda x: x is True, result))
            logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

            kquery.upsert()
            response['queries'].append(response_kquery)

        elif not kquery.is_stale(kq_result['last_modified']):
            # Hot / Hit
            logging.info("KQuery is HOT")
            for mts in MTS.from_cache(kq_result['mts_keys'], redis_client):
                response_kquery = mts.build_response(kquery, response_kquery)
            response['queries'].append(response_kquery)

        else:
            # Warm / Stale
            logging.info('KQuery is WARM')

            new_kairos_result = kquery.proxy_to_kairos(CONF_DICT['kairosdb']['host'],
                                                       CONF_DICT['kairosdb']['port'],
                                                       {'start_absolute': kq_result['last_modified']})

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
                    response_kquery = mts.build_response(kquery, response_kquery, trim=False)
                else:
                    old_mts.merge_from(mts, is_newer=True)
                    pipeline.set(old_mts.get_key(), json.dumps(old_mts.result), ex=old_mts.expiry)
                    # old_mts.upsert()
                    response_kquery = old_mts.build_response(kquery, response_kquery)
            result = pipeline.execute()
            success_count = len(filter(lambda x: x is True, result))
            logging.info("MTS write pipeline: %d of %d successful" % (success_count, len(result)))

            kquery.upsert()
            response['queries'].append(response_kquery)
    return json.dumps(response)
