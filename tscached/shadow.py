import logging


def should_add_to_readahead(config, referrer, headers):
    """ Should we add this KQuery for readahead behavior?
        :param config: dict representing the top-level tscached config
        :param referrer: str, from the http request
        :param headers: dict, all headers from the http request
        :return: boolean
    """
    if headers.get(config['shadow']['http_header_name'], None):
        return True

    for substr in config['shadow']['referrer_blacklist']:
        if substr in referrer:
            return False
    return True


def process_for_readahead(config, redis_client, kquery_key, referrer, headers):
    """ Couple this KQuery to readahead behavior. If Redis fails, eat the exception.
        :param config: dict representing the top-level tscached config
        :param redis_client: redis.StrictRedis
        :param kquery_key: str, usually tscached:kquery:HASH
        :param referrer: str, from the http request
        :param headers: dict, all headers from the http request
        :return: void:
        :raise: redis.exceptions.RedisError
    """
    if should_add_to_readahead(config, referrer, headers):
        resp = redis_client.sadd('tscached:shadow', kquery_key)
        logging.info('Shadow: Added %d key: %s' % (resp, kquery_key))
    else:
        logging.debug('Shadow: NOT adding key: %s' % kquery_key)
