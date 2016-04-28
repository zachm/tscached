import logging
import sys

from flask import Flask
import yaml

from tscached.utils import setup_logging


VERSION = '0.1.3'

app = Flask(__name__, static_url_path='', static_folder='kairos-web')

# Inject our custom YAML-based config into the Flask app.
try:
    ndx = sys.argv.index('tscached-config') + 1
    config_filename = sys.argv[ndx]
except:
    # debug / developement scenario
    config_filename = 'tscached.yaml'

try:
    with open(config_filename, 'r') as config_file:
        app.config['tscached'] = yaml.load(config_file.read())['tscached']
except IOError:
    logging.error('Webapp only: Could not read config file: %s.' % config_filename)


if not app.debug:
    setup_logging()


import tscached.handler_general
import tscached.handler_maintenance
import tscached.handler_meta
