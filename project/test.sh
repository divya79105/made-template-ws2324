#!/bin/bash

# Install Python packages
pip install pandas pysqlite3 kaggle opendatasets

# Navigate to the correct directory
cd /home/runner/work/made-template-ws2324/made-template-ws2324/

# Run the Python scripts
python3 project_pipeline.py
python3 test.py
