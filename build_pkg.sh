#!/bin/zsh 
#
# pip install setuptools wheel
#
python setup.py sdist bdist_wheel
pip install ./dist/crbuilder-0.4.4.tar.gz
