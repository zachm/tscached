import datetime
import logging

import simplejson as json

from utils import create_key
from utils import get_timetable
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
            return json.loads(result)
        else:
            logging.info('Cache MISS: %s' % self.get_key())
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


class MTS(DataCache):

    expiry = 10800  # three hours
    result = None

    def __init__(self, redis_client):
        super(MTS, self).__init__(redis_client, 'mts')

    @classmethod
    def from_result(cls, results, redis_client):
        # includes everything except sample_size, which we'll recalculate later
        for result in results['results']:
            new = cls(redis_client)
            new.result = result
            yield new

    @classmethod
    def from_cache(cls, redis_keys, redis_client):
        for redis_key in redis_keys:
            new = cls(redis_client)
            new.redis_key = redis_key
            new.result = new.get_cached()
            yield new


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
    query = None
    related_mts = None

    def __init__(self, redis_client):
        super(KQuery, self).__init__(redis_client, 'kquery')
        self.related_mts = set()

    @classmethod
    def from_request(cls, request, redis_client):
        """ Generator. HTTP query can create many KQueries.  """
        for metric in request.get('metrics', []):
            new = cls(redis_client)
            new.time_range = cls.populate_time_range(request)
            new.query = metric
            yield new

    @staticmethod
    def populate_time_range(request_dict):
        """ KQueries need to know their needful time interval, so we bring it inside them.  """
        relevant_keys = ['start_relative', 'end_relative', 'start_absolute', 'end_absolute']
        time_range = {}
        for key in relevant_keys:
            if key in request_dict:
                time_range[key] = request_dict[key]
        return time_range

    @staticmethod
    def derive_time_range(request):
        """ Create ms. timestamps related to a HTTP request's data. """

        ### TODO we don't support (we ignore) the time_zone input.
        now = datetime.datetime.now()
        start = None
        end = None
        if request.get('start_absolute'):
            start = int(request['start_absolute'])
        else:
            td = get_timedelta(request.get('start_relative'))
            start = int((now - td).strftime("%s")) * 1000

        if request.get('end_absolute'):
            end = int(request['end_absolute'])
        elif request.get('end_relative'):
            td = get_timedelta(request.get('end_relative'))
            end = int((now - td).strftime("%s")) * 1000
        else:
            end = None
        return (start, end)

    def key_basis(self):
        """ We already remove the timestamps and store them separately. """
        return self.query

    def proxy_to_kairos(self, time_range=None):
        if not time_range:
            proxy_query = self.time_range
        else:
            proxy_query = time_range

        proxy_query['metrics'] = [self.query]
        proxy_query['cache_time'] = 0
        return query_kairos(proxy_query)

    def upsert(self):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same. """
        value = self.query

        # This could be a separate Redis layer but I don't see how that's a win.
        value['mts_keys'] = [x.get_key() for x in self.related_mts]

        self.set_cached(value)

    def add_mts(self, mts):
        self.related_mts.add(mts)

#    def get_mts(self):
#        for mts in self.related_mts:
#            yield mts


