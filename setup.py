# -*- coding: utf-8 -*-

"""
A setuptools based setup module for Entsoe-py.

Adapted from
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the source code
with open(path.join(here, 'entsoe', 'entsoe.py'), encoding='utf-8') as f:
    lines = f.readlines()
    for l in lines:
        if l.startswith('__version__'):
            __version__ = l.split('"')[1] # take the part after the first "

setup(
    name='entsoe-py',
    version=__version__,
    description='A python API wrapper for ENTSO-E',
    long_description=long_description,
    url='https://github.com/EnergieID/entsoe-py',
    author='EnergieID.be',
    author_email='jan@energieid.be',
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    keywords='ENTSO-E data api energy',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #py_modules=[""],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.
    install_requires=['requests', 'pytz', 'beautifulsoup4', 'pandas'],

    # If there are data files included in your packages that need to be
    # installed, specify them here.  If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.
    # Note: for creating the source distribution, they had to be included in the
    # MANIFEST.in as well.
    package_data={
        'entsoe-py': ['LICENSE', 'README.md'],
    },
)
