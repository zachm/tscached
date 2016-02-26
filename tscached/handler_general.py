import datetime
import logging
import simplejson as json
import yaml

from flask import request
import redis

from tscached import app
from tscached import cache_calls
from tscached.kquery import KQuery
from tscached.utils import FETCH_AFTER
from tscached.utils import FETCH_ALL
from tscached.utils import FETCH_BEFORE
from tscached.utils import populate_time_range
from tscached.utils import get_needed_absolute_time_range
from tscached.utils import get_range_needed


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
    config = app.config['tscached']


    logging.info('Query')
    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    kairos_time_range = populate_time_range(payload)
    start_request, end_request = get_needed_absolute_time_range(kairos_time_range)
    response = {'queries': []}

    # HTTP request may contain one or more kqueries
    for kquery in KQuery.from_request(payload, redis_client):
        kq_result = kquery.get_cached()

        if kq_result:
            try:
                start_cache = datetime.datetime.fromtimestamp(float(kq_result['earliest_data']))
                end_cache = datetime.datetime.fromtimestamp(float(kq_result['last_add_data']))
            except:
                # some sort of cache malformation or error
                start_cache = None
                end_cache = None
            staleness_threshold = 10  # TODO static lookup in config
            range_needed = get_range_needed(start_request, end_request, start_cache,
                                            end_cache, staleness_threshold)
            merge_method = range_needed[2]
            if not range_needed:
                # hot / hit
                kq_resp = cache_calls.hot(redis_client, kquery, kairos_time_range)
            elif merge_method == FETCH_ALL:
                logging.info('Odd COLD scenario: data exists.')
                # cold / miss
                kq_resp = cache_calls.cold(config, redis_client, kquery, kairos_time_range)
            elif merge_method in [FETCH_BEFORE, FETCH_AFTER]:
                # warm / stale
                kq_resp = cache_calls.warm(config, redis_client, kquery, kairos_time_range,
                                           range_needed)
            else:
                logging.error("Received an unsupported range_needed value: %s" % range_needed[2])
                kq_resp = {}
        else:
            # complete redis miss: cold
            kq_resp = cache_calls.cold(config, redis_client, kquery, kairos_time_range)

        response['queries'].append(kq_resp)
    return json.dumps(response)
