#!/usr/bin/env bash
python3 setup.py sdist
python3 setup.py bdist_wheel
python3 setup.py sdist bdist_wheel upload