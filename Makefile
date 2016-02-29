# A very simple Makefile.
# tscached is only tested against python 2.7 (newest at press time was 2.7.11)
# It *might* be poked work with 2.6... but why are you still using 2.6?!?
# Feel free to change PYTHONEXEC if you need to.

PYTHONEXEC=python2.7
DEBUGPORT=8008

# builds the darn thing
all: venv

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
	rm -rf venv debug-run.py
	rm -rf build/ dist/ tscached.egg-info/ htmlcov/
	find . -name \*.pyc -delete

# Run with uWSGI server, multiple workers, etc.
run: venv
	source venv/bin/activate ; uwsgi --ini uwsgi.ini -H venv

# Run with single-threaded debug server. Flask gives you auto-reloading on code changes for free.
debug: venv
	echo "from tscached import app; app.debug = True; app.run(host='0.0.0.0', port=${DEBUGPORT})" > debug-run.py
	source venv/bin/activate; venv/bin/python debug-run.py
