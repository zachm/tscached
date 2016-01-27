import os
import simplejson as json
import yaml

from flask import make_response

from tscached import app


@app.route('/', methods=['GET'])
def handle_root():
    return "hello world!"
