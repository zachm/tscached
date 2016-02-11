import datetime
import hashlib

import requests
import simplejson as json

KAIROS_HOST = 'localhost'
KAIROS_PORT = 8080


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


def get_timedelta(value, raw=False):
    """ input has keys value, unit. common inputs noted start_relative, end_relative """
    seconds = int(value['value']) * SECONDS_IN_UNIT[value['unit']]
    if raw:
        return seconds
    return datetime.timedelta(seconds=seconds)


def query_kairos(query, raw=False):
    """ do it. """
    url = 'http://%s:%s/api/v1/datapoints/query' % (KAIROS_HOST, KAIROS_PORT)
    r = requests.post(url, data=json.dumps(query))
    if raw:
        return r.text
    else:
        return json.loads(r.text)


def create_key(data, tipo):
    """ data should be hashable (str, usually). tipo is str. """
    genHash = hashlib.sha224(data).hexdigest()
    key = "tscached:%s:%s" % (tipo, genHash)
    return key
