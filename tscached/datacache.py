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
        """ Does this entry exist in our cache? result/False. """
        result = self.redis_client.get(self.get_key())
        if result:
            logging.info('Cache HIT: %s' % self.get_key())
            return result
        else:
            logging.info('Cache MISS: %s' % self.get_key())
            return False

    def set_cached(self, value):
        result = self.redis_client.set(self.get_key(), value, ex=self.expiry)
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


class MTS(DataCache):

    expiry = 10800  # three hours
    result = None

    def __init__(self, redis_client):
        super(MTS, self).__init__(redis_client, 'mts')

    @classmethod
    def from_result(cls, result, redis_client):
        new = cls(redis_client)
        new.result = result
        return new

    def key_basis(self):
        mts_key_dict = {}
        mts_key_dict['tags'] = self.result['tags']
        mts_key_dict['group_by'] = self.result['group_by']
        mts_key_dict['name'] = self.result['name']
        return mts_key_dict

    def upsert(self):
        self.set_cached(self.result)


class KQuery(DataCache):

    expiry = 10800  # three hours, matching Kairos
    raw_query = None
    raw_result = None
    query = None
    related_mts = None

    def __init__(self, redis_client):
        super(KQuery, self).__init__(redis_client, 'kquery')
        self.related_mts = set()

    @classmethod
    def from_request(cls, payload, redis_client):
        new = cls(redis_client)
        new.raw_query = str(payload)
        new.query = json.loads(payload)
        return new

    def key_basis(self):
        """ Hashing on 'metrics' implies removing all start/end timestamps. """
        return self.query['metrics']

    def query_backend_for_result(self):
        self.raw_result = query_kairos(self.query, raw=True)
        return json.loads(self.raw_result)

    def get_raw_backend_result(self):
        """ To avoid an unnecessary reserialization. """
        return self.raw_result

    def upsert(self):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same. """
        value = self.query

        # This could be a separate Redis layer but I don't see how that's a win.
        value['mts_keys'] = [x.get_key() for x in self.related_mts]

        self.set_cached(value)

    def add_mts(self, mts):
        self.related_mts.add(mts)

