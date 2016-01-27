import os
import simplejson as json
import yaml

from flask import make_response
from flask import request
import requests

from tscached import app

KAIROS_HOST = 'localhost'
KAIROS_PORT = 8080

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
        query = json.loads(request.data)
    else:
        query = request.args.get('query')

    return json.dumps(query_kairos(query))
