import logging
import simplejson as json
import yaml

from flask import request
import redis
import requests

from tscached import app


"""
    NOTE: and TODO(?) The big logging block present in handler_general is missing here.
    This module receives it, in normal operation, afaict because it is imported in
    tscached/__init__.py *after* handler_general. This seems janky and should be remediated.
"""


with open('tscached.yaml', 'r') as config_file:
    CONF = yaml.load(config_file.read())['tscached']


def metadata_caching(config, key, endpoint):
    """ Encapsulate stupid-simple cache logic for Kairos "metadata" endpoints.
        config: dict, usually the contents of tscached.yaml.
        key: string, used for redis keying.
        endpoint: string, the corresponding kairosdb endpoint.
        returns: 2-tuple: (content, HTTP code)
    """
    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    get_result = redis_client.get('tscached:' + key)
    if get_result:  # hit. no need to process the JSON blob, so don't!
        logging.info('Meta Endpoint: %s: HIT' % key)
        return get_result, 200
    else:
        logging.info('Meta Endpoint: %s: MISS, GET, and SET' % key)
        url = 'http://%s:%s%s' % (config['kairosdb']['host'], config['kairosdb']['port'], endpoint)
        kairos_result = requests.get(url)
        if kairos_result.status_code != 200:
            # propagate the kairos message to the user along with its error code.
            logging.error('Meta Endpoint: %s: got %s from kairos: %s' % (key,
                          kairos_result.status_code, kairos_result.text))
        else:
            expiry = config['expiry'].get(key, 300)  # 5 minute default
            set_result = redis_client.set('tscached:' + key, kairos_result.text, ex=expiry)
            if not set_result:
                logging.error('Meta Endpoint: %s: Cache SET failed: %s' % (key, set_result))
        return kairos_result.text, kairos_result.status_code


@app.route('/api/v1/metricnames', methods=['GET'])
def handle_metricnames():
    return metadata_caching(CONF, 'metricnames', '/api/v1/metricnames')


@app.route('/api/v1/tagnames', methods=['GET'])
def handle_tagnames():
    return metadata_caching(CONF, 'tagnames', '/api/v1/tagnames')


@app.route('/api/v1/tagvalues', methods=['GET'])
def handle_tagvalues():
    return metadata_caching(CONF, 'tagvalues', '/api/v1/tagvalues')
