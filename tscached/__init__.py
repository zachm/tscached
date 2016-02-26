from flask import Flask
app = Flask(__name__)

import tscached.handler_general
import tscached.handler_meta
