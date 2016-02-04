import hashlib
import logging
import os
import simplejson as json
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
        raw_query = request.data  # str
        query = json.loads(raw_query)  # dict
    else:
        # TODO: We add an extra serialization. Maybe ok since we don't use GET much.
        # TODO: We cast to str for consistent hashing... this is ungood.
        raw_query = str(request.args.get('query'))
        query = json.loads(raw_query)

    logging.info('Query')

    redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

    kquery = KQuery.from_request(raw_query, redis_client)
    kquery_result = kquery.get_cached()

    if not kquery_result:
        # Cold / Miss
        kairos_result = kquery.query_backend_for_result()

        for result in kairos_result['queries']:
            for result in result['results']:

                mts = MTS.from_result(result, redis_client)
                kquery.add_mts(mts)
                mts.upsert()
        kquery.upsert()

        return kquery.get_raw_backend_result()
    else:
        logging.info('Redis HIT: %s' % kquery.get_key())
        # TODO update protocol. Like, how to merge two kairos results?


    return json.dumps(query_kairos(query))

