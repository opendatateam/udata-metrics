#!/usr/bin/env python

import os
import io
import re

from setuptools import setup, find_packages

RE_BADGE = re.compile(r'^\[\!\[(?P<text>.*?)\]\[(?P<badge>.*?)\]\]\[(?P<target>.*?)\]$', re.M)

BADGES_TO_KEEP = []


def md(filename):
    '''
    Load .md (markdown) file and sanitize it for PyPI.
    '''
    content = io.open(filename).read()

    for match in RE_BADGE.finditer(content):
        if match.group('badge') not in BADGES_TO_KEEP:
            content = content.replace(match.group(0), '')

    return content


def pip(filename):
    """Parse pip reqs file and transform it to setuptools requirements."""
    return open(os.path.join('requirements', filename)).readlines()


long_description = '\n'.join((
    md('README.md'),
    md('CHANGELOG.md'),
    ''
))

install_requires = pip('install.pip')
tests_require = pip('test.pip')


setup(
    name='udata-metrics',
    version='0.1.0',
    description='Metrics for uData',
    long_description=long_description,
    url='https://github.com/opendatateam/udata-metrics',
    author='Open Data Team',
    author_email='contact@opendata.team',
    packages=['udata_metrics'],
    include_package_data=True,
    python_requires='>=3.7',
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
    entry_points={
        'udata.models': [
            'metrics = udata_metrics.models',
        ],
        
        'udata.tasks': [
            'metrics = udata_metrics.tasks',
        ],
        
        'udata.plugins': [
            'metrics = udata_metrics',
        ],
    },
    license='AGPL',
    zip_safe=False,
    keywords='udata metrics',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Topic :: System :: Software Distribution',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
