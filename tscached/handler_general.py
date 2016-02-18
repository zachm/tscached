import logging
import simplejson as json
import yaml

from flask import request
import redis

from tscached import app
from tscached import cache_calls
from tscached.kquery import KQuery
from tscached.utils import populate_time_range
from tscached.utils import get_needed_absolute_time_range
from tscached.utils import get_range_needed


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
    kairos_time_range = populate_time_range(payload)
    start_request, end_request = get_needed_absolute_time_range(kairos_time_range)
    response = {'queries': []}

    # HTTP request may contain one or more kqueries
    for kquery in KQuery.from_request(payload, redis_client):
        kq_result = kquery.get_cached()
        range_needed = get_range_needed(start_request, end_request, kq_result)
        if not range_needed:
            # hot / hit
            kq_resp = cache_calls.hot(redis_client, kquery, kairos_time_range)
        elif range_needed[2] == 'overwrite':
            if kq_result:
                logging.info('Odd COLD scenario: data exists.')
            # cold / miss
            kq_resp = cache_calls.cold(CONF_DICT, redis_client, kquery, kairos_time_range)
        elif range_needed[2] in ['append', 'prepend']:
            # warm / stale
            kq_resp = cache_calls.warm(CONF_DICT, redis_client, kquery, kairos_time_range, range_needed)
        response['queries'].append(kq_resp)
    return json.dumps(response)
