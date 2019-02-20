"""Describe the modules for sharing"""
import os
import re
import sys
# this is standard Python packaging from http://python-packaging.readthedocs.io
from setuptools import setup

if sys.version_info < (3, 5):
    sys.exit('Sorry, Python < 3.5 is not supported')

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


def read(version_file):
    with open(os.path.join(SCRIPT_DIR, version_file)) as fp:
        content = fp.read()
    return content


def find_version(version_file):
    version_file = read(version_file)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name='async-process-pool',
    version=find_version('async-process-pool/__version__.py'),
    packages=['async-process-pool'],
    install_requires=[])
