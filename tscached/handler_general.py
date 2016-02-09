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
        response_kquery = {'results': [], 'sample_size': 0}
        if not kq_result:
            # Cold / Miss
            logging.debug('KQuery is COLD')

            kairos_result = kquery.proxy_to_kairos()
            # Loop over every MTS
            for mts in MTS.from_result(kairos_result['queries'][0], redis_client):
                kquery.add_mts(mts)
                mts.upsert()
                response_kquery = mts.build_response(kquery, response_kquery, trim=False)

            kquery.upsert()
            response['queries'].append(response_kquery)

        elif not kquery.is_stale(kq_result['last_modified']):
            # Hot / Hit
            logging.debug("KQuery is HOT")
            for mts in MTS.from_cache(kq_result['mts_keys'], redis_client):
                response_kquery = mts.build_response(kquery, response_kquery)
            response['queries'].append(response_kquery)

        else:
            # Warm / Stale
            logging.debug('KQuery is WARM')

            last_modified = kq_result['last_modified']
            new_kairos_result = kquery.proxy_to_kairos({'start_absolute': last_modified * 1000})

            cached_mts = {}  # redis key to MTS
            # pull in old MTS, put them in a lookup table
            for mts in MTS.from_cache(kq_result['mts_keys'], redis_client):
                kquery.add_mts(mts)  # we want to write these back eventually
                cached_mts[mts.get_key()] = mts

            # loop over newly returned MTS. if they already existed, merge/write. if not, just write.
            for mts in MTS.from_result(new_kairos_result['queries'][0], redis_client):
                logging.debug("Size of cached_mts: %d" % len(cached_mts.keys()))

                old_mts = cached_mts.get(mts.get_key())
                if not old_mts:  # would have been added in previous loop
                    kquery.add_mts(mts)
                    mts.upsert()
                    response_kquery = mts.build_response(kquery, reponse_kquery, trim=False)
                else:
                    old_mts.merge_from(mts, is_newer=True)
                    old_mts.upsert()
                    response_kquery = old_mts.build_response(kquery, response_kquery)

            kquery.upsert()
            response['queries'].append(response_kquery)
    return json.dumps(response)
#    return json.dumps(query_kairos(query))
