#!/usr/bin/env python
import argparse
import datetime
import sys

import requests
import simplejson as json


"""
    It's the start of something.
    Not actually automated testing, but a harness for comparing
    tscached and kairosdb outputs.

    At some point later in development, this is going to be part of a 
    full acceptance testing suite. That's the goal, anyway.
"""


def load_example_data(filepath):
    with open(filepath, 'r') as f:
        return json.loads(f.read())


def prettyprint_ts(ts, micro=True):
    its = int(ts)
    if micro:
        its /= 1000
    print datetime.datetime.fromtimestamp(its).strftime('%Y-%m-%d %H:%M:%S')


def query_with_get(url, query):
    r = requests.get(url, params={'query': json.dumps(query)})
    print r.url
    return json.loads(r.text)


def query_with_post(url, query):
    r = requests.post(url, data=json.dumps(query))
    return json.loads(r.text)


def summarize_results(query, results):
    assert results.keys() == ['queries']

    # because of input - one Q, one A
    assert len(results['queries']) == 1
    answer = results['queries'][0]
    # a given Q can return multiple TS - this one is a singleton
    assert len(answer['results']) == 1

    # the returned TS should match the overall sample_size
    # TODO: only if the testing kairos has been running the whole time.
    ts = answer['results'][0]
    try:
        assert answer['sample_size'] == len(ts['values'])
    except AssertionError:
        print "Sample size didn't match the length of values..."
        print "Sample size: %d; # values: %d" % (answer['sample_size'], len(ts['values']))

    num_ts = len(ts['values'])
    first_ts = ts['values'][0][0]
    last_ts = ts['values'][-1][0]

    print "%d values found, newest to oldest:\t%d\t%d" % (num_ts, first_ts, last_ts)
    prettyprint_ts(first_ts)
    prettyprint_ts(last_ts)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Harness for querying/testing tscached/kairosdb')
    parser.add_argument('-p', '--port', type=int, default=8080, help='port to query on (8080)')
    parser.add_argument('-s', '--server', type=str, default='localhost', help='hostname (localhost)')
    parser.add_argument('--verb', type=str, default='POST', help='GET or POST (default)')
    parser.add_argument('--analysis', action='store_true', default=False,
                        help='Run a summarize routine instead of barfing JSON.')
    parser.add_argument('--request', type=str, default='example_data/simple_query_request.json',
                        help='Use a different query, specify as JSON-formatted file.')
    args = parser.parse_args()

    request = load_example_data(args.request)
    request['start_relative'] = {'value': '1', 'unit': 'minutes'}


    url = 'http://%s:%d/api/v1/datapoints/query' % (args.server, args.port)
    if args.verb == 'POST':
        results = query_with_post(url, request)
    else:
        results = query_with_get(url, request)

    if not args.analysis:
        print json.dumps(results)
    else:
        summarize_results(request, request)
