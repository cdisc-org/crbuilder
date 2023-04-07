#!/bin/zsh 
#
python setup.py sdist bdist_wheel
pip install ./dist/crbuilder-0.3.1.tar.gz
