import logging
import simplejson as json

from flask import request
import redis
import requests

from tscached import app
from tscached.utils import create_key


"""
    NOTE: and TODO(?) The big logging block present in handler_general is missing here.
    This module receives it, in normal operation, afaict because it is imported in
    tscached/__init__.py *after* handler_general. This seems janky and should be remediated.
"""


def metadata_caching(config, name, endpoint, post_data=None):
    """ Encapsulate stupid-simple cache logic for Kairos "metadata" endpoints.
        config: nested dict loaded from the 'tscached' section of a yaml file.
        name: string, used as a part of redis keying.
        endpoint: string, the corresponding kairosdb endpoint.
        post_data: None or string. overrides default GET proxy behavior. implies custom keying.
        returns: 2-tuple: (content, HTTP code)
    """
    if post_data:
        redis_key = create_key(post_data, name)
    else:
        redis_key = 'tscached:' + name

    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    get_result = redis_client.get(redis_key)

    if get_result:  # hit. no need to process the JSON blob, so don't!
        logging.info('Meta Endpoint HIT: %s' % redis_key)
        return get_result, 200
    else:
        logging.info('Meta Endpoint MISS: %s' % redis_key)
        url = 'http://%s:%s%s' % (config['kairosdb']['host'], config['kairosdb']['port'], endpoint)
        if post_data:
            kairos_result = requests.post(url, data=post_data)
        else:
            kairos_result = requests.get(url)

        if kairos_result.status_code != 200:
            # propagate the kairos message to the user along with its error code.
            logging.error('Meta Endpoint: %s: got %s from kairos: %s' % (redis_key,
                          kairos_result.status_code, kairos_result.text))
        else:
            expiry = config['expiry'].get(name, 300)  # 5 minute default
            set_result = redis_client.set(redis_key, kairos_result.text, ex=expiry)
            if not set_result:
                logging.error('Meta Endpoint: %s: Cache SET failed: %s' % (redis_key, set_result))
        return kairos_result.text, kairos_result.status_code


@app.route('/api/v1/metricnames', methods=['GET'])
def handle_metricnames():
    return metadata_caching(app.config['tscached'], 'metricnames', '/api/v1/metricnames')


@app.route('/api/v1/tagnames', methods=['GET'])
def handle_tagnames():
    return metadata_caching(app.config['tscached'], 'tagnames', '/api/v1/tagnames')


@app.route('/api/v1/tagvalues', methods=['GET'])
def handle_tagvalues():
    return metadata_caching(app.config['tscached'], 'tagvalues', '/api/v1/tagvalues')


@app.route('/api/v1/datapoints/query/tags', methods=['POST'])
def handle_metaquery():
    return metadata_caching(app.config['tscached'], 'metaquery', '/api/v1/datapoints/query/tags',
                            request.data)
