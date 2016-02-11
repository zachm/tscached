import datetime
import logging
import time

from datacache import DataCache
from utils import get_timedelta
from utils import query_kairos


KAIROS_HOST = 'localhost'
KAIROS_PORT = 8080


class KQuery(DataCache):

    expiry = 10800  # three hours, matching Kairos
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
            new.populate_time_range(request)
            new.query = metric
            yield new

    def populate_time_range(self, request_dict):
        """ KQueries need to know their time interval, so we bring it inside them.  """
        relevant_keys = ['start_relative', 'end_relative', 'start_absolute', 'end_absolute']
        self.time_range = {}
        for key in relevant_keys:
            if key in request_dict:
                self.time_range[key] = request_dict[key]

    def get_needed_absolute_time_range(self):
        """ Create datetimes from HTTP-type data. Gives 2-tuple (start, end). end can be None. """

        # TODO we don't support the time_zone input, also millisecond resolution.
        now = datetime.datetime.now()
        start = None
        end = None
        if self.time_range.get('start_absolute'):
            start = datetime.datetime.fromtimestamp(int(self.time_range['start_absolute']) / 1000)
        else:
            td = get_timedelta(self.time_range.get('start_relative'))
            start = now - td

        if self.time_range.get('end_absolute'):
            end = datetime.datetime.fromtimestamp(int(self.time_range['end_absolute']) / 1000)
        elif self.time_range.get('end_relative'):
            td = get_timedelta(self.time_range.get('end_relative'))
            end = now - td
        else:
            end = None

        return (start, end)

    def is_stale(self, last_modified, staleness_threshold=10):
        """ Boolean: Is the returned data too old?
                last_modified: a millisecond unix timestamp pulled out of a cached kquery structure
                staleness_threshold: number of seconds until a HOT query needs updating
        """
        start, end = self.get_needed_absolute_time_range()
        now = datetime.datetime.now()
        last_modified = datetime.datetime.fromtimestamp(int(last_modified / 1000))

        if end:
            if end < now and last_modified < now and last_modified > end:
                return False  # defined end, updated afterwards, cached afterwards
            if (now - end) <= datetime.timedelta(seconds=staleness_threshold):
                return False  # defined end, cached recently
        else:
            if (now - last_modified) <= datetime.timedelta(seconds=staleness_threshold):
                return False  # end at NOW, updated recently enough

        return True

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
        kairos_result = query_kairos(KAIROS_HOST, KAIROS_PORT, proxy_query)
        if len(kairos_result['queries']) != 1:
            logging.error("Proxy expected 1 KQuery result, found %d" % len(kairos_result['queries']))
        return kairos_result

    def upsert(self):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same. """
        # This could be a separate Redis layer but I don't see how that's a win.
        self.query['mts_keys'] = [x.get_key() for x in self.related_mts]
        # Use as a sentinel to check for WARM vs HOT
        self.query['last_modified'] = time.time() * 1000
        self.set_cached(self.query)

    def add_mts(self, mts):
        self.related_mts.add(mts)
