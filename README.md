# Data Test B - Deployment

This is a working solution for Sephora SEA data test parts B2 and B3. Part A is available in the following repo: [data-test-sde-alan-A](https://github.com/emailayuen/data-test-sde-alan-A).

## Approach
B2: Utilise the same code base created in section A, but swap out fake scripts with real SQL scripts that will be run against Google Bigquery.

B3: Expose API endpoints using 2 x AWS services: API Gatway + Lambda (serverless code)

## Deploy and Run Locally

### Prerequisites

* [Python 3.7](https://www.python.org/downloads/) (3.6 should suffice but untested)
* [Google BigQuery API client library for Python](https://cloud.google.com/bigquery/docs/reference/libraries)
* Key file for service account to access Google Bigquery (...to be provided separately)

### Setup

To run the program...

1. Install the Google Bigquery client library in a terminal.
  
  ```
  Example:
  
  pip install --upgrade google-cloud-bigquery
  ```
  
2. Pull down the source code to your local machine and navigate to the main project folder.
  
  ```
  Example:
  
  cd C:\git\data-test-sde-alan-B
  ```
  
3. Once key file is obtained, paste the key file into the `key` folder directly under the main project folder. This is required to authenticate with Google Bigquery instance.
  
  ```
  Example:
  
  C:\git\data-test-sde-alan-B\key\[data-test-sde-app-key.json]
  
  ```
  
  4. To run, simply pass the warehouseLoadLambdaFunction.py program as an argument to the Python interpreter.

  ```
  Example:
  
  C:\git\data-test-sde-alan-B>python warehouseLoadLambdaFunction.py
  ```

### Setup with Custom Bigquery Instance

You can also use your own Bigquery instance if desired...

1. Repeat steps 1-2 above.

2. Set up a project in Google Cloud and create a service account that can create, read and write into your custom Bigquery instance. Generate a `key file` for your service account.

3. Place your custom `key file` into the `key` folder directly under the main project folder.

  ```
  Example:
  
  C:\git\data-test-sde-alan-B\key\[your_custom_key_file.json]
  ```
4. For `warehouseLoadLambdaFunction.py`, modify the constant variables `PROJECT_ID` and `KEY_FILE_PATH` with respective details of your custom Bigquery instance

  ```
  Example:
  
  PROJECT_ID = '[your_custom_project_id]'
  KEY_FILE_PATH = 'key/[your_custom_key_file.json]'
  ```
5. For `warehouseInsertLambdaFunction.py`, do the same as step 4.

  
