# Data Test B - Deployment

This is a working solution for Sephora SEA data test parts B2 and B3. Part A is available in the following repo: [data-test-sde-alan-A](https://github.com/emailayuen/data-test-sde-alan-A).

## Requirements

* [Python 3.7](https://www.python.org/downloads/) (3.6 should suffice but untested)
* [Google BigQuery API client library for Python](https://cloud.google.com/bigquery/docs/reference/libraries)
* Key file for service account to access Google Bigquery (...to be provided separately)

## Setup

To run the program...

  1. Pull down the source code to your local machine and navigate to the project folder.
  
  ```
  Example:
  
  cd C:\git\data-test-sde-alan-B
  
  ```
  
  2. Once key file is obtained, paste the key file into the key folder directly under the project folder.
  
  ```
  Example:
  
  Paste key file into: C:\git\data-test-sde-alan-B\key\
  
  ```
  
  3. To run, simply pass the warehouseLoadLambdaFunction.py program as an argument to the Python interpreter.

```
  Example:
  
  C:\git\data-test-sde-alan-B>python warehouseLoadLambdaFunction.py
  
