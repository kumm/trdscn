#!/bin/bash

cd cloudformation
pipenv run make
cd ../
cd functions/loader-binance
pipenv requirements > requirements.txt
cd ../../
cd functions/loader-fmp
pipenv requirements > requirements.txt
cd ../../
sam build
