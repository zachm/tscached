import logging


def should_add_to_readahead(config, referrer, headers):
    """ Should we add this KQuery for readahead behavior?
        config: dict representing the top-level tscached config
        referrer: str, from the http request
        headers: dict, all headers from the http request
        returns: boolean
    """
    if headers.get(config['shadow']['http_header_name'], None):
        return True

    for substr in config['shadow']['referrer_blacklist']:
        if substr in referrer:
            return False
    return True


def populate_for_readahead(config, redis_client, kquery_key, referrer, headers):
    """ Couple this KQuery to readahead behavior.
        config: dict representing the top-level tscached config
        redis_client: redis.StrictRedis
        kquery_key: str, usually tscached:kquery:HASH
        referrer: str, from the http request
        headers: dict, all headers from the http request
        returns: void
    """
    if should_add_to_readahead(config, referrer, headers):
        resp = redis_client.sadd('tscached:shadow', kquery_key)
        logging.info('Shadow: Added %d key: %s' % (resp, kquery_key))
    else:
        logging.debug('Shadow: NOT adding key: %s' % kquery_key)
