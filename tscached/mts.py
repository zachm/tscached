import logging

from datacache import DataCache


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

    def merge_from(self, new_mts, is_newer=True):
        """ Merge new_mts into this one
            Default behavior appends new data; is_newer=False prepends old data.
            This assumes the MTS match on cardinality.
            This does not write back to Redis (must call upsert!)
        """
        if is_newer:
            ctr = 0
            while True:
                # compare newest timestamp to eldest timestamp in extension.
                # we take preference on the cached data.
                if self.result['values'][-1][0] < new_mts.result['values'][ctr][0]:
                    break
                ctr += 1
                # arbitrary cutoff
                if ctr > 10:
                    logging.error('Could not conduct merge: %s' % self.get_key())
                    return
            if ctr > 0:
                logging.debug('Trimmed %d values from new update on MTS %s' % (ctr, self.get_key()))
            self.result['values'].extend(new_mts.result['values'][ctr:])
        else:
            ctr = -1
            while True:
                # compare eldest timestamp to newest timestamp in extension.
                # we take preference on the cached data.
                if self.result['values'][0][0] > new_mts.result['values'][ctr][0]:
                    break
                ctr -= 1
                # arbitrary cutoff
                if ctr < -10:
                    logging.error('Could not conduct merge: %s' % self.get_key())
                    return
            if ctr == -1:
                self.result['values'] = new_mts.result['values'] + self.result['values']
            elif ctr < -1:
                logging.debug('Trimmed %d values from old update on MTS %s' % (ctr, self.get_key()))
                self.result['values'] = new_mts.result['values'][ctr] + self.result['values']
            else:
                logging.error('Backfill somehow had a positive offset!')

    def trim(self, start, end=None):
        """ Return a subset of the MTS to give back to the user.
            start, end are datetimes
            We assme the MTS already contains all needed data.
        """
        RESOLUTION_SEC = 10

        last_ts = self.result['values'][-1][0]
        ts_size = len(self.result['values'])
        start = int(start.strftime('%s'))

        # (ms. difference) -> sec. difference -> 10sec. difference
        start_from_end_offset = int((last_ts - (start * 1000)) / 1000 / RESOLUTION_SEC)
        start_from_start_offset = ts_size - start_from_end_offset - 1  # off by one

        if not end:
            logging.debug('Trimming: from_end is %d, from_start is %d' % (start_from_end_offset,
                          start_from_start_offset))
            return self.result['values'][start_from_start_offset:]

        end = int(end.strftime('%s'))
        end_from_end_offset = int((last_ts - (end * 1000)) / 1000 / RESOLUTION_SEC)
        end_from_start_offset = ts_size - end_from_end_offset
        logging.debug('Trimming (mid value): start_from_end is %d, end_from_end is %d' %
                      (start_from_end_offset, end_from_end_offset))
        return self.result['values'][start_from_start_offset:end_from_start_offset]

    def build_response(self, kquery, response_dict, trim=True):
        """ Mutates internal state and returns it as a dict.
            This should be the last method called in the lifecycle of MTS objects.
            kquery - the kquery this result belongs to.
            response_dict - the accumulator.
            trim - to trim or not to trim.
        """
        if trim:
            start_trim, end_trim = kquery.get_needed_absolute_time_range()
            logging.debug('Trimming: %s, %s' % (start_trim, end_trim))
            self.result['values'] = self.trim(start_trim, end_trim)
        response_dict['sample_size'] += len(self.result['values'])
        response_dict['results'].append(self.result)
        return response_dict
