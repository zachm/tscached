import logging
import os
import simplejson as json

from flask import request
import redis

from tscached import app
from tscached.cache_calls import cold
from tscached.cache_calls import process_cache_hit
from tscached.kquery import KQuery
from tscached.shadow import process_for_readahead
from tscached.utils import BackendQueryFailure
from tscached.utils import populate_time_range


@app.route('/', methods=['GET'])
def handle_root():
    dir_of_this_file = os.path.dirname(os.path.realpath(__file__))
    if os.path.exists(dir_of_this_file + '/kairos-web/index.html'):
        return app.send_static_file('index.html')
    return ("Welcome to tscached! If you're looking for a web frontend, try running `make frontend`"
            " and restarting the server.")


@app.route('/api/v1/health/check', methods=['GET'])
def handle_healthcheck():
    """ kairosdb responds 204 No Content, so that's what we will do here. """
    return ('', 204)


@app.route('/api/v1/datapoints/query', methods=['POST', 'GET'])
def handle_query():
    try:
        if request.method == 'POST':
            payload = json.loads(request.data)  # dict
        else:
            payload = json.loads(request.args.get('query'))
    except:
        err = 'Cannot deserialize JSON payload.'
        logging.error(err)
        return json.dumps({'error': err}), 500

    config = app.config['tscached']

    logging.info('Query')
    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    kairos_time_range = populate_time_range(payload)
    ret_data = {'queries': []}
    overall_cache_mode = None

    # HTTP request may contain one or more kqueries
    for kquery in KQuery.from_request(payload, redis_client):
        try:
            # get whatever is in redis for this kquery
            kq_result = kquery.get_cached()

            # readahead shadow load support
            process_for_readahead(config, redis_client, kquery.get_key(), request.referrer,
                                  request.headers)
            if kq_result:
                kq_resp, cache_mode = process_cache_hit(config, redis_client, kquery, kairos_time_range)
            else:
                kq_resp = cold(config, redis_client, kquery, kairos_time_range)
                cache_mode = 'cold_miss'
        except BackendQueryFailure as e:
            # KairosDB is broken so we fail fast.
            logging.error('BackendQueryFailure: %s' % e.message)
            return json.dumps({'error': e.message}), 500
        except redis.exceptions.RedisError as e:
            # Redis is broken, so we pretend it's a cache miss. This will eat any further exceptions.
            logging.error('RedisError: ' + e.message)
            kq_resp = cold(config, redis_client, kquery, kairos_time_range)
            cache_mode = 'cold_proxy'
        ret_data['queries'].append(kq_resp)

        if not overall_cache_mode:
            overall_cache_mode = cache_mode
        elif cache_mode != overall_cache_mode:
            overall_cache_mode = 'mixed'

    return json.dumps(ret_data), 200, {'Content-Type': 'application/json', 'X-tscached-mode': overall_cache_mode}
