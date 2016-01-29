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

KAIROS_HOST = 'localhost'
KAIROS_PORT = 8080
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


def query_kairos(query):
    """ do it. """
    url = 'http://%s:%s/api/v1/datapoints/query' % (KAIROS_HOST, KAIROS_PORT)
    r = requests.post(url, data=json.dumps(query))
    return json.loads(r.text)


def create_key(data, tipo):
    """ data should be hashable (str, usually). tipo (ES, 'type') is str: 'mts' is used right now. """
    genHash = hashlib.sha224(data).hexdigest()
    key = "tscached:%s:%s" % (tipo, genHash)
    logging.debug("generated redis key: %s" % key)
    return key


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

    key = create_key(raw_query, 'mts')
    client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
    redis_result = client.get(key)

    if not redis_result:
        logging.info('Redis   MISS: %s' % key)
        kairos_result = query_kairos(query)
        json_result = json.dumps(kairos_result)
        # TODO custom expiry (seconds)
        # TODO does nx make sense? (only set if DNE)
        res = client.set(key, raw_query, ex=3600, nx=True)
        logging.info('Redis    SET: %s %s' % (res, key))
        return json_result
    else:
        logging.info('Redis    HIT: %s' % key)
        # TODO update protocol. Like, how to merge two kairos results?


    return json.dumps(query_kairos(query))
