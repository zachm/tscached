#!/usr/bin/env python
from __future__ import absolute_import
import os

import argparse
import redis
import yaml

from tscached.shadow import perform_readahead


def start():
    parser = argparse.ArgumentParser(description='Harness for querying/testing tscached/kairosdb')

    parser.add_argument('-c', '--config', type=str, default=os.path.abspath('tscached.yaml'),
                        help='Path to config file.')
    args = parser.parse_args()

    with open(args.config, 'r') as config_file:
        config = yaml.load(config_file.read())['tscached']

    redis_client = redis.StrictRedis(host=config['redis']['host'], port=config['redis']['port'])
    perform_readahead(config, redis_client)


if __name__ == '__main__':
    start()
