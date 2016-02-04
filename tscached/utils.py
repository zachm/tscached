import hashlib
import logging

import requests
import simplejson as json

KAIROS_HOST='localhost'
KAIROS_PORT=8080



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
    logging.debug("generated redis key: %s" % key)
    return key
