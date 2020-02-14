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

Turn on whichever the DAG you want to run.

## Configurations
Our connectors uses configurations that are provided by the user on the `Airflow` UI interface.

### Setting the Variables Manually
To add these Variables, naviagte to the `Admin -> Variables - ADD/+`

Depending on DAG that you've activated, add the following Variables



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

`SURV_FORMS = ''` :-  A list of forms which submissions should be fetched

Below is how the form list should look
````
"SURV_FORMS": [
        {
            "name": "name of the form (table name uses this so it should not have spaces)",
            "unique_column": "PRIMARY KEY COLUMN (used for references so, the column must be unique)",
            "form_id": "form ID as on SurveyCTO survers",
            "statuses": [
                "approved",
                "pending",
                "rejected"
            ],
            "last_date": "Oct%2013%2C%202015%2000%3A00%3A00%20AM":- url-encoded or epoc,
            "fields": [
              {
                "name": "field_1",
                "type": "data_type (VAR:-for less than 100 chars, Text:- for long texts)"
              },
              {
                "name": "first_name",
                "type": "var:"
              },
              {
                "name": "description",
                "type": "text"
              }
    
            ]
        }
	]
````
`SURV_DBMS = ''` :- Database management system to dump the data (mongo/mongodb, postgres/postgresdb)

`SURV_RECREATE_DB = ''` :- set if you want new tables to be always created isntead of updating the values

## Upload the Variables from a json file
Download the `variables_template.json` on this repo and update it with the correct values.

Navigate to the `Admin -> Variables`

Click on Import Variables after selecting the valid JSON file.

NOTE: For this option to work, the file must be a valid flat json file as the one in the template.