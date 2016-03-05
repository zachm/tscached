from flask import Flask
import yaml

from tscached.utils import setup_logging


app = Flask(__name__, static_url_path='', static_folder='kairos-web')

# Inject our custom YAML-based config into the Flask app.
with open('tscached.yaml', 'r') as config_file:
    app.config['tscached'] = yaml.load(config_file.read())['tscached']

if not app.debug:
    setup_logging()


import tscached.handler_general
import tscached.handler_meta
