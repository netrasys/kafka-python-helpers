#!/usr/bin/env python

from setuptools import setup
from setuptools.command.test import test as TestCommand
import codecs
import os
import sys
import re

HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """Return multiple read calls to different readable objects as a single
    string."""
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(HERE, *parts), 'r').read()

LONG_DESCRIPTION = read('README.rst')


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = [
            '--strict',
            '-vv',
            '--tb=long',
            'tests']
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='kafka-python-helpers',
    version='0.6.3',
    url='http://github.com/netrasys/kafka-python-helpers',
    license='Netra Inc. Proprietary',
    author='Sorin Otescu',
    install_requires=[
        'six==1.11.0',
        'kafka-python==1.4.3',
        'colorama==0.3.9'
    ],
    tests_require=['pytest', 'pytest-cov'],
    cmdclass={'test': PyTest},
    author_email='sotescu@netra.io',
    description='Extra functionality for Kafka',
    long_description=LONG_DESCRIPTION,
    scripts=[
        'scripts/kafka_consumer.py',
        'scripts/kafka_flush_topic.py',
        'scripts/kafka_producer.py'
    ],
    packages=['kafka_python_helpers'],
    include_package_data=True,
    platforms='any',
    test_suite='tests',
    zip_safe=False,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX :: Linux',
        'Topic :: System :: Networking',
        ],
)
