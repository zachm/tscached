# -*- coding: utf-8 -*-
# Always prefer setuptools over distutils
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

setup(
    name='tscached',
    version='0.0.1',

    description='Caching proxy for time series data',
    long_description='Advanced caching proxy for KairosDB, using Redis as a datastore.',

    url='https://github.com/zachm/tscached',

    author='Zach Musgrave',
    author_email='ztm@zachm.us',
    license='License :: OSI Approved :: GNU General Public License v3 (GPLv3)',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Framework :: Flask',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 2.7',
    ],

    keywords='metrics proxy caching kairosdb redis',

    packages=find_packages(exclude=['tests']),

    install_requires=[],

    extras_requires={
        'testing': ['mock'],
    },
)
