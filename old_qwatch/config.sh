#!/usr/bin/env bash

# MCSR configuration of pipenv and a user based pip
wget https://bootstrap.pypa.io/get-pip.py
python3.6 get-pip.py --user
rm get-pip.py
echo "export PATH=\"\$HOME/.local/bin:\$PATH\"" >> .bash_profile

pip install pipenv --user