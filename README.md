# connectors
A flexible data integration tool to help nonprofits connect to their data collection tools and ERP systems

## Available Connectors (Airflow DAGs)
1. SurveyCTO
2. ComCare
3. ONA

With all the above connectors, the pipeline does the following operations:
- Fetch data from the servers
- Dump the data into Postgres or MongoDB
- Sync server and database everytime the pipeline runs (remove stale data)

## Obtaining the DAGs
After installing Airflow, or setting it up on your environment, 

clone the DAGs into your `dags` folder by using these commands:

- For HTTPs: `https://github.com/hikaya-io/connectors.git dags`
- For SSH: `git@github.com:hikaya-io/connectors.git dags`

## Enable the DAGs
Navigate to your DAGs page on the`Airflow` UI and refresh to see the new cloned DAGs from `connectors` repo

Turn on whichever DAG you want to run.

## Configurations
Our connectors uses configurations that are provided by the user on the `Airflow` UI interface.

### Setting the Variables Manually
To add these Variables, naviagte to the `Admin -> Variables - ADD/+`

Depending on which DAG you've activated, add the following Variables:

#### Postgress Settings
`POSTGRES_DB_PASSWORD = ''` 

`POSTGRES_DB = ''`

`POSTGRES_HOST = ''`

`POSTGRES_USER = ''`

`POSTGRES_PORT = ''`

####  MongoDB Settings
`MONGO_DB_USER = ''`

`MONGO_DB_PASSWORD = ''`

`MONGO_DB_HOST = ''`

`MONGO_DB_PORT = ''`

#### SurveyCTO Variables #
`SURV_SERVER_NAME = ''` :- SurveyCTO server name

`SURV_USERNAME = ''` :- SurveyCTO login username (email address)

`SURV_PASSWORD =  ''` :- SurveyCTO login password

`SURV_DBMS = ''` :- Database management system to dump the data (mongo/mongodb, postgres/postgresdb)

## Upload the Variables from a json file
Download the `variables_template.json` on this repo and update it with the correct values.

Navigate to the `Admin -> Variables`

Click on Import Variables after selecting the valid JSON file.

> For this option to work, the file must be a valid flat JSON file as the one in the template.

## Trigger the DAG
To manually trigger the configured DAG:

 - Click on the DAG to open the DAG details
 - Click on `Trigger DAG`
 - Confirm triggering by clicking `ok` on the prompt
 
 If all the settings are correct, then your DAG should run successfully.

 ## Docker Airflow Setup (Optional)
> Even though it is posible to install [Apache Airflow](https://airflow.apache.org/docs/stable/start.html) `pip`, we have to perform more configurations to ensure it is production ready. Deploying Airflow via Docker is definititely faster especially using the image [puckel/docker-airflow](https://hub.docker.com/r/puckel/docker-airflow); for more information check out [docker-airflow](https://github.com/puckel/docker-airflow) Github repo.

### Requirements
- `docker` and `docker-compose` installed. (Create a [Docker Ubuntu DO Droplet](https://marketplace.digitalocean.com/apps/docker))
- create directory structure `/home/hikaya/dags`

### Setup
- Copy all files in the `DAGs` directory into the `dags` directory created above
- Navigate to the `Docker` directory and run the command below to deploy Airflow:

  ```docker-compose -f docker-compose-CeleryExecutor.yml up -d```
  
- To create an admin user:
  * execute an interactive bash shell on the Airflow webserver container:
  
  ```docker exec -it docker-airflow_webserver_1 bash```
  
  * use `airflow create_user` [CLI](https://airflow.apache.org/docs/stable/cli-ref#create_user) to create an admin user
  
  ```airflow create_user  --role Admin --username admin --email admin --firstname admin --lastname admin --password admin```
  
- Login using the user created above and load the variables
