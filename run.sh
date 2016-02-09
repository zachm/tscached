#!/bin/bash

source venv/bin/activate
uwsgi --ini uwsgi.ini -H venv
