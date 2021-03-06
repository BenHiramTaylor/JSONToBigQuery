#  JSON To BigQuery Microservice
This is a Go microservice designed to ingest raw unknown JSON data, parse it into a flat Avro structure, and then load it into a BigQuery Table.

### Note: I am sure this can be done better, i will be improving over time, but this is my first GO project, and i am doing it to gain experience with the language.
## Overview
Install by cloning down the repo and either using in your own docker/compose enviroment, or pushing to Kubernetes using the kubernetes.yaml file.
```shell
git clone https://github.com/BenHiramTaylor/JSONToBigQuery.git
```

## Link to GCP
Update the containers enviroment variables to map "GOOGLE_APPLICATION_CREDENTIALS" to the file path of your JSON key.
(You may need to edit the accounts permission, but the endpoint should report that.)

## Usage
Post the following JSON blob format to the endpoint:
```json
{
    "ProjectID": "big-swordfish-1120", 
    "DatasetName": "TestDataSet", 
    "TableName": "TestTable", 
    "IdField": "pID", 
    "Query": "CREATE OR REPLACE TABLE TABLENAME AS (SELECT DISTINCT * FROM TABLENAME)",
    "Data": [
        {
            "pID": 1,
            "ListMapTest":[
                "A",
                "C"
            ]
        },
        {
            "pID": 2,
            "NewValue": "b",
            "ExampleNest": {
                "NestedKey": 3,
                "DoubleNest": {
                    "DOUBLENESTVAL": null
                }
            },
            "ListMapTest":[
                "B",
                "C"
            ]
        },
        {
            "pID": 3,
            "NewValue": null,
            "ExampleNest": {
                "NestedKey": null
            },
            "ListMapTest":[
                "A",
                "B"
            ]
        }
    ]
}
```
#### Fields
- ProjectID: Your GCP project that contains the BigQuery enviroment you wish to load to.
- DatasetName: The name of the dataset, this will be created if it does not already exist.
- TableName: The name of the table, this will be created if it does not already exist.
- IdField: The field in your raw parsed JSON that representes the "id" of your obeject, used later for de-duplication and parsing lists into a different table.
- Query: A query to run immediatly after the load, can be for de-duplication, merging results or frankly anything you need, Leave out of body to run no query
- Data: A list of the raw JSON objects you wish to parse, one object equals one row in BigQuery, this will be parsed into a flat structure in the case of nested dictionaries, and lists will be mapped by the key and id into a different table.
  
FIELDS CAN BE LEFT OUT, AND THEY WILL BE NULLED ON THE BigQuery SIDE AS SEEN BELOW.

## Notes
- If you are going to use the kubernetes.yaml and cloudbuild.yaml files then update the YOUR-PROJECT-NAME-HERE and YOUR-CLUSTER-NAME-HERE with the project the cluster is stored in and the cluster name for the CD deployment.