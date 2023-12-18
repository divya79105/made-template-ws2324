#!/bin/bash

# Install Python packages
pip install pandas
pip install pysqlite3
pip install kaggle
pip install opendatasets
python3 project_pipeline.py
python3 test.py
