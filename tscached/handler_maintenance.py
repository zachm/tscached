import logging

from flask import request
import redis
import simplejson as json

from tscached import VERSION
from tscached import app
from tscached import shadow


@app.route('/api/maintenance/flushall', methods=['GET'])
def handle_flushall():
    """ Clears the entire Redis cache.
        Will cowardly refuse to act if the callee does not provide orly=yarly in the GET string.
        If we were truly RESTful this would not be method GET, but it's intended for ease of use.
        :return: 200 response, dict with key 'message' describing success/failure/cowardice.
    """
    config = app.config['tscached']
    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    orly = request.args.get('orly')

    if orly == 'yarly':
        # Amusingly, we don't need to release the lock. It's implicit by virtue of deleting everything!
        lock = shadow.become_leader(config, redis_client)
        if lock:
            logging.info('Flushall acquired shadow lock')
            redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
            ret = redis_client.flushall()
            message = 'Redis FLUSHALL executed; received response: %s' % ret
        else:
            message = 'Could not acquire shadow lock. Is shadow load taking place? (Or just try again.)'
    else:
        message = 'Cowardly refusing to act, add orly=yarly to execute Redis FLUSHALL.'

    return json.dumps({'message': message}), 200


@app.route('/version', methods=['GET'])
def handle_version():
    return VERSION, 200
