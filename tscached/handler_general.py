import datetime
import hashlib
import logging
import os
import simplejson as json
import time
import yaml

from flask import make_response
from flask import request
import redis
import requests

from tscached import app
from tscached.datacache import KQuery
from tscached.datacache import MTS
from tscached.utils import create_key
from tscached.utils import query_kairos

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

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

def series_equivalent(a, b):
    """ Given dicts from .queries[].results[].* - verify if they match one another.
        Much of this is O(n^2) - we presume n will be rather small.
    """
    # metric names must be the same
    if a['name'] != b['name']:
        return False

    # same number of tags
    if len(a['tags']) != len(b['tags']):
        return False
    for tag_key in a['tags']:
        if a[tag_key] != b[tag_key]:
            return False

    # same number of groupings
    if len(a['group_by']) != len(b['group_by']):
        return False
    for grouping in a['group_by']:
        if grouping not in b['group_by']:
            return False
    return True


def create_timeseries_key(result):
    """ Given a result (single TS dict), return a hash describing its semantics. """

    big_concat = result['name'] + '::'

    for tag in sorted(result['tags'].keys()):
        big_concat += tag + ':' + sorted(result['tags'][tag]).join(',')
    big_concat += '::'

    groupings = []
    for grouping in result['group_by']:
        grouping_str = ''
        for key in sorted(grouping.keys()):
            grouping_str += '%s.%s' % (key, grouping[key])
        groupings.append(grouping_str)
    big_concat += '::' + sorted(groupings).join(',')

    return 'tscached::mts::' + hashlib.sha224(big_concat).hexdigest()






@app.route('/api/v1/datapoints/query', methods=['POST', 'GET'])
def handle_query():
    if request.method == 'POST':
        payload = json.loads(request.data)  # dict
    else:
        payload = json.loads(request.args.get('query'))

    logging.info('Query')
    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    response = {'queries': []}

    # HTTP request may contain one or more kqueries
    for kquery in KQuery.from_request(payload, redis_client):
        kq_result = kquery.get_cached()
        if not kq_result:
            # Cold / Miss
            kairos_result = kquery.proxy_to_kairos()
            if len(kairos_result['queries']) != 1:
                logging.error("Proxy expected 1 KQuery result, found %d" % len(kairos_result['queries']))
            query_result = kairos_result['queries'][0]

            # Loop over every MTS
            response_kquery = {'results': [], 'sample_size': 0}
            for mts in MTS.from_result(query_result, redis_client):
                kquery.add_mts(mts)
                mts.upsert()

                response_kquery['sample_size'] += len(mts.result['values'])
                response_kquery['results'].append(mts.result)

            kquery.upsert()
            response['queries'].append(response_kquery)

        elif kquery.is_stale(kq_result['last_modified']):
            logging.debug("KQuery is WARM")
        else:
            logging.debug("KQuery is HOT")
            response_kquery = {'results': [], 'sample_size': 0}
            for mts in MTS.from_cache(kq_result['mts_keys'], redis_client):
                response_kquery['sample_size'] += len(mts.result['values'])
                response_kquery['results'].append(mts.result)
            response['queries'].append(response_kquery)

    return json.dumps(response)



#    return json.dumps(query_kairos(query))

