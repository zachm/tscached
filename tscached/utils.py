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
