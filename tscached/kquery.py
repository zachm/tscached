import copy
import datetime
import logging
import threading

from datacache import DataCache
from utils import BackendQueryFailure
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

            # This seemingly terrifying kludge makes it easy to merge chunks of MTS together.
            # For all aggregators, we remove align_sampling and add align_start_time.
            # Otherwise, partial windows (if aggregated) will appear on every seam in the data. Info:
            # https://kairosdb.github.io/docs/build/html/restapi/QueryMetrics.html#metric-properties
            for agg_ndx in xrange(len(new.query.get('aggregators', []))):
                if 'align_sampling' in new.query['aggregators'][agg_ndx]:
                    del new.query['aggregators'][agg_ndx]['align_sampling']
                    new.query['aggregators'][agg_ndx]['align_start_time'] = True

            yield new

    @classmethod
    def from_cache(cls, redis_keys, redis_client):
        """ Generator. Given redis keys, yield KQueries. """
        pipeline = redis_client.pipeline()
        for key in redis_keys:
            pipeline.get(key)
        results = pipeline.execute()
        for ctr in xrange(len(redis_keys)):
            new = cls(redis_client)
            new.redis_key = redis_keys[ctr]
            new.query = new.process_cached_data(results[ctr])
            new.cached_data = new.query  # emulating get_cached behavior
            yield new

    def key_basis(self):
        """ We already remove the timestamps and store them separately. """
        return self.query

    def proxy_to_kairos(self, host, port, time_range):
        """ Send this KQuery to Kairos with a custom time range and get the response.
            :param host: str, kairosdb host.
            :param port: int, kairosdb port.
            :param time_range: dict, usually generated via utils.populate_time_range
            :return: dict, the response from kairos
            :raise: utils.BackendQueryFailure, if the query fails.
        """
        proxy_query = copy.deepcopy(time_range)
        proxy_query['metrics'] = [self.query]
        proxy_query['cache_time'] = 0

        kairos_result = query_kairos(host, port, proxy_query)

        if len(kairos_result['queries']) != 1:
            logging.error("Proxy expected 1 KQuery result, found %d" % len(kairos_result['queries']))
        return kairos_result

    def proxy_to_kairos_chunked(self, host, port, time_ranges, timeout=30):
        """ Send this KQuery to Kairos in chunks of custom time ranges.
            :param host: str, kairosdb host.
            :param port: int, kairosdb port.
            :param time_ranges: list of 2-tuples of datetime.datetime. new to old.
            :return: dict, int->dict. key is index of entry in time_ranges; value is kairos response.
            :raise: utils.BackendQueryFailure, if the query fails.
        """
        results = {}
        threads = []

        def _thread_wrap(ndx, query):
            results[ndx] = query_kairos(host, port, query, propagate=False)

        ndx = 0
        for time_range in time_ranges:
            # Build a full thread, with query, out of each chunk of time.
            start_ts = int(time_range[0].strftime('%s')) * 1000
            end_ts = int(time_range[1].strftime('%s')) * 1000
            query = {'start_absolute': start_ts, 'end_absolute': end_ts, 'cache_time': 0}
            query['metrics'] = [self.query]

            # Create the thread, keep a reference to it, and start it off.
            thr = threading.Thread(target=_thread_wrap, args=(ndx, query))
            threads.append(thr)
            thr.start()
            ndx += 1

        for thr in threads:  # Wait for all threads to finish.
            thr.join(timeout)

        for val in results.values():  # Quick and dirty exception propagation.
            if 'error' in val:
                raise BackendQueryFailure('KairosDB responded %d: %s' % (val.get('status_code', 0),
                                          val.get('message', 'no error given')))

        return results

    def upsert(self, start_time, end_time):
        """ Write the KQuery into Redis. Overwrites, writes, all treated the same.
            :param start_time: datetime.datetime, when we *began to ask* for data.
            :param end_time: datetime.datetime, when we *stopped asking* for data.
            :return: void
        """
        # This could be a separate Redis layer but I don't see how that's a win.
        self.query['mts_keys'] = [x.get_key() for x in self.related_mts]
        # Use as a sentinel to check for WARM vs HOT
        if end_time:
            self.query['last_add_data'] = int(end_time.strftime('%s'))
        else:
            self.query['last_add_data'] = int(datetime.datetime.now().strftime('%s'))
        self.query['earliest_data'] = int(start_time.strftime('%s'))
        self.set_cached(self.query)

    def add_mts(self, mts):
        """ Add MTS to be associated with this KQuery.
            :param mts: mts.MTS object
            :return: void
        """
        self.related_mts.add(mts)
