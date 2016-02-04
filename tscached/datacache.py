import logging

import simplejson as json

from utils import create_key
from utils import query_kairos


class DataCache(object):
    redis_key = None  # set in make_key
    cache_type = None
    redis_client = None

    def __init__(self, redis_client, cache_type):
        self.redis_client = redis_client
        self.cache_type = cache_type

    def get_cached(self):
        """ Does this KQuery exist in our cache? result/False. """
        if not self.redis_key:
            self.make_key()

        result = self.redis_client.get(self.redis_key)
        if result:
            logging.info('Cache HIT: %s' % self.redis_key)
            return result
        else:
            logging.info('Cache MISS: %s' % self.redis_key)
            return False

    def make_key(self):
        """ Create a key.
            Hashing on 'metrics' implies removing all start/end timestamps.
        """
        hashable = json.dumps(self.key_basis())
        self.redis_key = create_key(hashable, self.cache_type)

    def key_basis(self):
        """ Override this to describe what goes into a key's hash. """
        return {}


class KQuery(DataCache):

    expiry = 10800  # three hours, matching Kairos
    raw_query = None
    query = None
    mts_keys = []

    def __init__(self, redis_client):
        super(DataCache, self).__init(redis_client, 'kquery')
        pass

    @classmethod
    def from_request(cls, payload, redis_client):

        new = KQuery(redis_client)
        new.raw_query = str(payload)
        new.query = json.loads(payload)
        new.redis_client = rediscli
        return new

    def key_basis(self):
        return self.query['metrics']

    def insert(self):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same. """
        value = self.query

        # This could be a separate Redis layer but I don't see how that's a win.
        value['mts_keys'] = mts_keys

        result = self.redis_client.set(self.redis_key, value, ex=self.expiry)
        logging.info('Cache SET %s %s' % (result, self.redis_key))
