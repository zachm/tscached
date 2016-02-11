import logging

import simplejson as json

from utils import create_key


class DataCache(object):
    redis_key = None  # set in make_key
    cache_type = None
    redis_client = None

    def __init__(self, redis_client, cache_type):
        self.redis_client = redis_client
        self.cache_type = cache_type

    def get_cached(self):
        """ Does this entry exist in our cache? result/False. """
        return self.process_cached_data(self.redis_client.get(self.get_key()))

    def process_cached_data(self, result):
        """ Abstracted from get_cached because of pipelining. """
        if result:
            logging.debug('Cache HIT: %s' % self.redis_key)
            return json.loads(result)
        else:
            logging.debug('Cache MISS: %s' % self.redis_key)
            return False

    def set_cached(self, value):
        result = self.redis_client.set(self.get_key(), json.dumps(value), ex=self.expiry)
        if not result:
            logging.error('Cache SET failed: %s %s' % (result, self.get_key()))
        else:
            logging.info('Cache SET %s %s' % (result, self.get_key()))

    def get_key(self):
        if not self.redis_key:
            self.make_key()
        return self.redis_key

    def make_key(self):
        """ Create a key. """
        hashable = json.dumps(self.key_basis())
        self.redis_key = create_key(hashable, self.cache_type)

    def key_basis(self):
        """ Override this to describe what goes into a key's hash. """
        return {}
