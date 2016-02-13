import datetime
import hashlib

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


def get_timedelta(value):
    """ input has keys value, unit. common inputs noted start_relative, end_relative """
    seconds = int(value['value']) * SECONDS_IN_UNIT[value['unit']]
    return datetime.timedelta(seconds=seconds)


def query_kairos(kairos_host, kairos_port, query):
    """ do it. """
    url = 'http://%s:%s/api/v1/datapoints/query' % (kairos_host, kairos_port)
    r = requests.post(url, data=json.dumps(query))
    return json.loads(r.text)


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
