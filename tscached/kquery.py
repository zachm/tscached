import copy
import datetime
import logging
import time

from datacache import DataCache
from utils import query_kairos


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
            new.query = metric
            yield new

    def get_staleness_threshold(self):
        """ Build in request throttling here: big queries refresh less often. """
        return 10000  # ms

    def get_range_needed(self, start_request, end_request):
        last_add_data = datetime.datetime.fromtimestamp(int(self.query['last_add_data'] / 1000))
        earliest_data = datetime.datetime.fromtimestamp(int(self.query['earliest_data'] / 1000))
        have_earliest = False
        have_latest = False
        if earliest_data <= start_request:
            have_earliest = True
        if last_add_data >= end_request:
            have_latest = True

        if have_earliest and have_latest:
            # woo! we got it all!
            return False
        elif have_earliest and not have_latest:
            # we have early data, but not all recent data (TODO staleness)
            return (last_add_data, end_request)
        elif not have_earliest and have_latest:
            # we have all recent data, but not earlier data
            return (start_request, earliest_data)
        else:
            # we have no data, or only a small amount in the middle that we will overwrite.
            return (start_request, end_request)

    def key_basis(self):
        """ We already remove the timestamps and store them separately. """
        return self.query

    def proxy_to_kairos(self, host, port, time_range):
        """ time_range can be generated via utils.populate_time_range """
        proxy_query = copy.deepcopy(time_range)
        proxy_query['metrics'] = [self.query]
        proxy_query['cache_time'] = 0
        kairos_result = query_kairos(host, port, proxy_query)
        if len(kairos_result['queries']) != 1:
            logging.error("Proxy expected 1 KQuery result, found %d" % len(kairos_result['queries']))
        return kairos_result

    def upsert(self, start_time, end_time):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same. """
        # This could be a separate Redis layer but I don't see how that's a win.
        self.query['mts_keys'] = [x.get_key() for x in self.related_mts]
        # Use as a sentinel to check for WARM vs HOT
        self.query['last_add_data'] = end_time
        self.query['earliest_data'] = start_time
        self.set_cached(self.query)

    def add_mts(self, mts):
        self.related_mts.add(mts)
