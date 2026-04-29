#!/bin/bash
set -eo pipefail

git clone https://github.com/ECCO-GROUP/ECCO-Dataset-Production.git
git clone https://github.com/ECCO-GROUP/ECCO-v4-Configurations.git

python3 -m venv venv
source venv/bin/activate
pip install ECCO-Dataset-Production
