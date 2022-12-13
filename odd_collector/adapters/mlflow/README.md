# ml-flow 

## Preview:
 - [Keyword Definition](#keyword-definition)
 - [Available parameters](#available-parameters)
 - [Current client flow](#current-client-flow)
 - [Job info specifics](#job-info-specifics)

## Keyword Definition
Experiment (pipeline) - is a sequence of jobs (runs).

Job (run) - is a transformation task/ method that is applied to input data.

Experiment can be composed of several jobs or a single one. Job can include one or a set of transformations.

## Available Parameters
Each Experiment in mlflow has:

| Parameter                              |                                                                         
|----------------------------------------|
| artifact_location                      |                                              
| creation_time                          |                                             
| experiment_id                          |                                                   
| last_update_time                       |                                         
| lifecycle_stage                        |                                                  
| name                                   |                                                   
| tags                                   |                                                                                              |                                                |                                                 |

Each Run(job) in mlflow has:

| Parameter                  |                                                                         
|----------------------------|
| metrics                    |                                              
| params                     |                                             
| tags                       |                                                   
| last_update_time           |                                         
| lifecycle_stage            |                                                  
| artifact_uri               |                                                   
| end_time                   |
| experiment_id              | 
| lifecycle_stage            | 
| run_id                     | 
| run_name                   | 
| run_uuid                   | 
| start_time                 | 
| status                     | 
| user_id                    | 


## Current client flow
MlFlow client returns two type of entities, they are Experiment Entity and Job Entity.

1. Client connects to mlflow tracking uri by link that is set in [config](https://github.com/opendatadiscovery/odd-collector/blob/40d218a0ab8d0644884f06b5b55577094577ba48/odd_collector/domain/plugin.py) as host.

2. Client requests necessary experiments by name in case the list was specified in [config](https://github.com/opendatadiscovery/odd-collector/blob/40d218a0ab8d0644884f06b5b55577094577ba48/odd_collector/domain/plugin.py) as pipelines,
otherwise we request the full list of experiments.

3. For each experiment fetch list of jobs and general information with the list of parameters that could be found [here](odd_collector/adapters/mlflow/domain/job.py) 

## Job info specifics

1. Request list of jobs for each experiment_id.
(Experiment's jobs are requested as a dataframe. List of jobs (runs) that belongs to specified Experiment is generated while rows iteration)

2. For each job_id get job_info (run_info) and request list of artifacts. 
(As we can't fetch separately input/ output artifacts, request list of artifacts for each job. 
In case it is a folder iterate though files and append each one to a list.)

If it is necessary to specify exactly which artifacts are input/output log them in your experiment as a params with key 'input_artifacts' and 'output_artifacts',

For instance:  mlflow.log_param('input_artifacts',['https://'])

