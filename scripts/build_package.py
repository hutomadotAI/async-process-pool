"""Script to build code"""
import argparse
import os
from pathlib import Path
import sys
import subprocess


class Error(Exception):
    """Base exception for this module"""
    pass


class PytestError(Error):
    """Version error"""
    pass


class VersionError(Error):
    """Version error"""
    pass


def python_package(package_name, version, package_root):
    """Does the python packaging step"""
    _check_version(version)

    # write out the version to file so that it is persisted
    version_file = package_root / package_name / '__version__.py'
    with version_file.open('w') as fp:
        print('__version__ = "{}"'.format(version), file=fp)

    # build the source distribution, using a sub-process of Python
    cmdline = [sys.executable, 'setup.py', "sdist"]
    subprocess.run(cmdline, cwd=str(package_root), check=True)


def _check_version(version):
    if '_' in version:
        raise VersionError('Version name has invalid characters - {}'.format(version))


SCRIPT_PATH = Path(os.path.dirname(os.path.realpath(__file__)))
ROOT_DIR = SCRIPT_PATH.parent


def main(build_args):
    """Main function"""
    src_path = ROOT_DIR / "src"
    python_package(
        'async_process_pool',
        build_args.version,
        src_path)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description='Python utilities package command-line')
    PARSER.add_argument('version', help='version number')
    PARSER.add_argument('--upload', help='Upload to ???', action="store_true")

    BUILD_ARGS = PARSER.parse_args()
    main(BUILD_ARGS)