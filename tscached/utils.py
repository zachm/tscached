import datetime
import hashlib

import redis
import requests
import simplejson as json


# note: this doesn't work perfectly for months (31 days) or years (365 days)
SECONDS_IN_UNIT = {
                   'seconds': 1,
                   'minutes': 60,
                   'hours': 3600,
                   'days': 86400,
                   'weeks': 604800,
                   'months': 2678400,
                   'years': 31536000
                  }


# constants used in get_range_needed
FETCH_BEFORE = 'prepend'
FETCH_AFTER = 'append'
FETCH_ALL = 'overwrite'


class BackendQueryFailure(requests.exceptions.RequestException):
    """ Raised if the backing TS database (KairosDB) fails. """
    pass


class CacheQueryFailure(redis.exceptions.RedisError):
    """ Raised if the backing cache (Redis) fails. """
    pass


def get_timedelta(value):
    """ input has keys value, unit. common inputs noted start_relative, end_relative """
    seconds = int(value['value']) * SECONDS_IN_UNIT[value['unit']]
    return datetime.timedelta(seconds=seconds)


def query_kairos(kairos_host, kairos_port, query):
    """ As the name states.
        kairos_host: str, host/fqdn of kairos server. commonly a load balancer.
        kairos_port: int, port that kairos (or a proxy) listens on.
        query: dict to send to kairos.
        returns: dict containing kairos' response.
        raises: BackendQueryFailure if the operation doesn't succeed.
    """
    try:
        url = 'http://%s:%s/api/v1/datapoints/query' % (kairos_host, kairos_port)
        r = requests.post(url, data=json.dumps(query))
        value = json.loads(r.text)
        if r.status_code / 100 != 2:
            message = ', '.join(value.get('errors', ['No message given']))
            raise BackendQueryFailure('KairosDB responded %d: %s' % (r.status_code, message))
        return value
    except requests.exceptions.RequestException as e:
        raise BackendQueryFailure('Could not connect to KairosDB: %s' % e.message)


def create_key(data, tipo):
    """ data should be hashable (str, usually). tipo is str. """
    genHash = hashlib.md5(data).hexdigest()
    key = "tscached:%s:%s" % (tipo, genHash)
    return key


def populate_time_range(request_dict):
    """ Filter a Kairos HTTP request for only its temporal members. """
    relevant_keys = ['start_relative', 'end_relative', 'start_absolute', 'end_absolute']
    time_range = {}
    for key in relevant_keys:
        if key in request_dict:
            time_range[key] = request_dict[key]
    return time_range


def get_needed_absolute_time_range(time_range):
    """ Create datetimes from HTTP-type data. Gives 2-tuple (start, end). end can be None. """

    # TODO we don't support the time_zone input, also millisecond resolution.
    now = datetime.datetime.now()
    start = None
    end = None
    if time_range.get('start_absolute'):
        start = datetime.datetime.fromtimestamp(int(time_range['start_absolute']) / 1000)
    else:
        td = get_timedelta(time_range.get('start_relative'))
        start = now - td

    if time_range.get('end_absolute'):
        end = datetime.datetime.fromtimestamp(int(time_range['end_absolute']) / 1000)
    elif time_range.get('end_relative'):
        td = get_timedelta(time_range.get('end_relative'))
        end = now - td
    else:
        end = None

    return (start, end)


def get_range_needed(start_request, end_request, start_cache, end_cache, staleness_threshold=10):
    """ What range of data should be proxied to KairosDB?
        start_request: datetime, earliest data the user requested
        end_request: datetime, latest data the user requested
        start_cache: datetime, earliest data known by redis KQuery repn.
        end_cache: datetime, latest data known by redis KQuery repn.
        staleness_threshold: int, # of seconds we should serve stale data for. Used for throttling.
        Returns: one of two scenarios
            3-tuple: (datetime start_proxy, datetime end_proxy, str type_of_update).
            False: no action needed; data we have is enough (or > enough) to fulfill the request.
    """
    if not end_request:
        end_request = datetime.datetime.now()

    if not start_cache or not end_cache:
        return (start_request, end_request, FETCH_ALL)
    have_earliest = False
    have_latest = False
    if start_cache <= start_request:
        have_earliest = True
    if end_cache >= end_request:
        have_latest = True

    if have_earliest and have_latest:
        # woo! we got it all!
        return False
    elif have_earliest and not have_latest:
        # we have early data, but not all recent data. compare to staleness threshold.
        if (end_request - end_cache) < datetime.timedelta(seconds=staleness_threshold):
            return False
        return (end_cache, end_request, FETCH_AFTER)
    elif not have_earliest and have_latest:
        # we have all recent data, but not earlier data
        return (start_request, start_cache, FETCH_BEFORE)
    else:
        # we have no data, or only a small amount in the middle that we will overwrite.
        return (start_request, end_request, FETCH_ALL)
