import hashlib
import logging
import os
import simplejson as json
import yaml

from flask import make_response
from flask import request
import requests

from tscached import app

KAIROS_HOST = 'localhost'
KAIROS_PORT = 8080


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

    logging.warn('whatever!')
    genHash = hashlib.sha224(raw_query).hexdigest()
    logging.debug("generated hash: %s" % genHash)

    return json.dumps(query_kairos(query))
