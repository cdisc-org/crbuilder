#!/bin/zsh 
#
python setup.py sdist bdist_wheel
pip install ./dist/crbuilder-0.4.3.tar.gz
