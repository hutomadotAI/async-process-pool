"""Describe the modules for sharing"""
import os
import re
import sys
from pathlib import Path
# this is standard Python packaging from http://python-packaging.readthedocs.io
from setuptools import setup

if sys.version_info < (3, 7):
    sys.exit('Sorry, Python < 3.7 is not supported')

SCRIPT_DIR = Path(os.path.abspath(os.path.dirname(__file__)))


def find_version(version_file):
    with version_file.open() as fp:
        version_content = fp.read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_content, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


with (SCRIPT_DIR / "README.md").open() as fp:
    long_description = fp.read()

setup(
    name='async_process_pool',
    version=find_version(SCRIPT_DIR / 'async_process_pool' / '__version__.py'),
    description="Async Process Pool and Queue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=['async_process_pool'],
    url='https://github.com/hutomadotAI/async-process-pool',
    author='Paul Annetts',
    author_email='paul@hutoma.ai',
    install_requires=[],
    classifiers=[
        "Development Status :: 4 - Beta", "Framework :: AsyncIO",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only", "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent"
    ])
