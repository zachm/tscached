# A very simple Makefile.
# tscached is only tested against python 2.7 (newest at press time was 2.7.11)
# It *might* be poked work with 2.6... but why are you still using 2.6?!?
# Feel free to change PYTHONEXEC if you need to.

PYTHONEXEC=python2.7
DEBUGPORT=8008

# turns out 'source' isn't a thing in sh.
SHELL=/usr/bin/env bash

# builds the darn thing
all: venv frontend

venv/bin/activate: requirements.txt
	test -d venv || virtualenv venv -p ${PYTHONEXEC}
	venv/bin/pip install -Ur requirements.txt
	venv/bin/python setup.py install

venv: venv/bin/activate

devbuild: venv
	venv/bin/pip install -Ur requirements-dev.txt

test: devbuild
	venv/bin/tox

cover: devbuild
	venv/bin/tox -e coverage

clean:
	rm -rf venv debug-run.py kairosdb/ tscached/kairos-web/
	rm -rf build/ dist/ tscached.egg-info/ htmlcov/
	find . -name \*.pyc -delete

# Run with uWSGI server, multiple workers, etc.
run: venv frontend
	source venv/bin/activate ; uwsgi --ini tscached.uwsgi.ini --wsgi-file tscached/uwsgi.py -H venv

# Run with single-threaded debug server. Flask gives you auto-reloading on code changes for free.
debug: venv frontend
	echo "from tscached import app; app.debug = True; app.run(host='0.0.0.0', port=${DEBUGPORT})" > debug-run.py
	source venv/bin/activate; venv/bin/python debug-run.py

# Copy the debug frontend from kairosdb into our project
frontend: venv
	test -d kairosdb || git clone https://github.com/kairosdb/kairosdb.git
	test -d tscached/kairos-web || cp -R kairosdb/webroot tscached/kairos-web
	cp logo/favicon.png tscached/kairos-web/img/favicon.png
	cp logo/logo.png tscached/kairos-web/img/logo.png
	cp logo/logo.png tscached/kairos-web/img/logoSmall.png
