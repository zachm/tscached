from flask import Flask
import yaml


app = Flask(__name__)

# Inject our custom YAML-based config into the Flask app.
with open('tscached.yaml', 'r') as config_file:
    app.config['tscached'] = yaml.load(config_file.read())['tscached']


import tscached.handler_general
import tscached.handler_meta
