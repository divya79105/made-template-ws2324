#!/bin/bash

# Install Python packages
pip install pandas pysqlite3 kaggle opendatasets

# Navigate to the correct directory
cd /home/runner/work/made-template-ws2324/made-template-ws2324/project/

# Print the current directory for debugging
pwd

# List files in the current directory for debugging
ls

# Run the Python script
python3 project_pipeline.py
