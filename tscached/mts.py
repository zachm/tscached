import copy
import logging

from datacache import DataCache
from utils import get_needed_absolute_time_range


class MTS(DataCache):

    def __init__(self, redis_client):
        super(MTS, self).__init__(redis_client, 'mts')
        self.result = None

        # TODO make these configurable
        self.expiry = 10800  # three hours
        self.acceptable_skew = 6
        self.expected_resolution = 10

    @classmethod
    def from_result(cls, results, redis_client):
        # includes everything except sample_size, which we'll recalculate later
        for result in results['results']:
            new = cls(redis_client)
            new.result = result
            yield new

    @classmethod
    def from_cache(cls, redis_keys, redis_client):
        pipeline = redis_client.pipeline()
        for key in redis_keys:
            pipeline.get(key)
        results = pipeline.execute()

        for ctr in xrange(len(redis_keys)):
            new = cls(redis_client)
            new.redis_key = redis_keys[ctr]
            new.result = new.process_cached_data(results[ctr])
            yield new

    def key_basis(self):
        mts_key_dict = {}
        mts_key_dict['tags'] = self.result['tags']
        if self.result.get('group_by'):
            mts_key_dict['group_by'] = self.result['group_by']
        if self.result.get('aggregators'):
            mts_key_dict['aggregators'] = self.result['aggregators']
        mts_key_dict['name'] = self.result['name']
        return mts_key_dict

    def upsert(self):
        self.set_cached(self.result)

    def merge_at_end(self, new_mts, cutoff=10):
        """ Append one MTS to the end of another. Remove up to cutoff values from end of cached MTS. """
        reverse_offset = -1
        first_new_ts = new_mts.result['values'][0][0]
        while True:

            # in rare occasions the cached data is too short. delete it.
            if reverse_offset * -1 > len(self.result['values']):
                self.result['values'] = new_mts.result['values']
                return

            old_ts_at_offset = self.result['values'][reverse_offset][0]
            if old_ts_at_offset < first_new_ts:
                break

            if reverse_offset < cutoff * -1:
                logging.debug('Could not merge_at_end; not updating: %s' % self.get_key())
                return
            reverse_offset -= 1

        # last cached value is older than first new value
        # remove the last, first values. they're often incorrect due to partial windowing.
        # TODO? be more careful here? a 1m rollup can show missing data; a 10s rollup behaves well.
        if reverse_offset == -1:
            self.result['values'] = self.result['values'][:-1] + new_mts.result['values'][1:]
        else:
            logging.debug('Sliced %d outdated values from end of cache: MTS %s' %
                          (reverse_offset, self.get_key()))
            self.result['values'] = self.result['values'][:reverse_offset + 1] + new_mts.result['values'][1:]

    def merge_at_beginning(self, new_mts, cutoff=10):
        """ Append new_mts to the beginning of this one.
            Remove up to cutoff values from beginning of cached MTS if they conflict.
            May raise IndexError if merge fails.
        """
        forward_offset = 0
        last_new_ts = new_mts.result['values'][-1][0]
        while True:
            # in rare occasions the cached data is too short. delete it.
            if forward_offset >= len(self.result['values']):
                self.result['values'] = new_mts.result['values']
                return

            old_ts_at_offset = self.result['values'][forward_offset][0]
            if old_ts_at_offset > last_new_ts:
                break

            if forward_offset >= cutoff:
                logging.debug('Could not merge_at_beginning; not updating: %s' % self.get_key())
                return
            forward_offset += 1

        if forward_offset > 1:
            logging.debug('Sliced %d outdated values from beginning of cache: MTS %s' %
                          (forward_offset, self.get_key()))
        logging.debug('COMPLETED!!!')
        self.result['values'] = new_mts.result['values'] + self.result['values'][forward_offset:]

    def robust_trim(self, start, end=None):
        """ This is a silly trim algorithm. Full O(n), but very robust.
            start: datetime of range start
            end: datetime of range end, or None if returning until NOW.
            Returns: generator of 2-ary lists that match start, end constraints.
        """
        data = self.result['values']
        start = int(start.strftime('%s'))
        if end:
            end = int(end.strftime('%s'))
        for entry in data:
            if (entry[0] / 1000) >= start:
                if not end:
                    yield entry
                elif (entry[0] / 1000) <= end:
                    yield entry

    def efficient_trim(self, start, end=None):
        """ Return a subset of the MTS to give back to the user.
            start, end are datetimes.
            We assume several notable things:
            - The MTS matches the resolution given. (TODO)
            - The MTS is consecutive; i.e., there are few or no gaps in the data.
            - The MTS has already been fetched from Redis.
            Given these constraints, efficient_trim represents a 5-10x speedup (indexing by offsets)
            over robust_trim (full-text search).
        """

        last_ts = self.result['values'][-1][0]
        ts_size = len(self.result['values'])
        start = int(start.strftime('%s'))
        # (ms. difference) -> sec. difference -> 10sec. difference
        start_from_end_offset = int((last_ts - (start * 1000)) / 1000 / self.expected_resolution)
        start_from_start_offset = ts_size - start_from_end_offset - 1  # off by one

        if not end:
            logging.debug('Trimming: from_end is %d, from_start is %d' % (start_from_end_offset,
                          start_from_start_offset))
            return self.result['values'][start_from_start_offset:]

        end = int(end.strftime('%s'))
        end_from_end_offset = int((last_ts - (end * 1000)) / 1000 / self.expected_resolution)
        end_from_start_offset = ts_size - end_from_end_offset
        logging.debug('Trimming (mid value): start_from_end is %d, end_from_end is %d' %
                      (start_from_end_offset, end_from_end_offset))
        return self.result['values'][start_from_start_offset:end_from_start_offset]

    def conforms_to_efficient_constraints(self):
        """ Can we use the efficient trim strategy? returns boolean. """
        first_ts = self.result['values'][0][0]
        last_ts = self.result['values'][-1][0]
        count = len(self.result['values'])

        # elapsed ms -> elapsed seconds -> downsampling based on configured resolution
        expected_count = (last_ts - first_ts) / 1000 / self.expected_resolution
        # how the number of points we have differs from what our 'perfect world' would have
        observed_skew = abs(expected_count - count)
        if observed_skew <= self.acceptable_skew:
            return True
        return False

    def build_response(self, time_range, response_dict, trim=True):
        """ TODO should refactor this to a better pattern.
            Mutates internal state and returns it as a dict.
            This should be the last method called in the lifecycle of MTS objects.
            response_dict - the accumulator.
            trim - to trim or not to trim.
        """
        new_values = None
        if trim:
            start_trim, end_trim = get_needed_absolute_time_range(time_range)

            if self.conforms_to_efficient_constraints():
                logging.debug('Efficient trimming: %s, %s' % (start_trim, end_trim))
                new_values = self.efficient_trim(start_trim, end_trim)
            else:
                logging.debug('Robust trimming: %s, %s' % (start_trim, end_trim))
                new_values = list(self.robust_trim(start_trim, end_trim))

            # shallow copy just at the first level of the dict
            new_result = copy.copy(self.result)
            new_result['values'] = new_values
            response_dict['sample_size'] += len(new_result['values'])
            response_dict['results'].append(new_result)
        else:
            response_dict['sample_size'] += len(self.result['values'])
            response_dict['results'].append(self.result)
        return response_dict
