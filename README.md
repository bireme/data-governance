# FI-Admin Data Migration to MongoDB
==============================

This project aims to migrate data from FI-Admin in JSON format and then convert it to MongoDB, enriching some metadata during the process. The project uses Python and Airflow to automate the migration.

## Objective
-----------

The primary objective is to provide an automated solution for migrating data from FI-Admin to MongoDB, ensuring that the data is enriched with relevant metadata during the process.

## Technologies Used
-------------------------

- **Python**: Used to develop the functions for extracting, transforming, and loading the data.
- **Airflow**: Used to automate and orchestrate the workflow of the migration.
- **MongoDB**: NoSQL database where the data will be stored after migration.
- **FI-Admin API**: Source of the data to be migrated.

## Features
------------

- **Data Extraction**: Extraction of JSON data from FI-Admin via API.
- **Transformation and Metadata Enrichment**: Transformation and enrichment with additional metadata.
- **Loading into MongoDB**: Loading of transformed data into MongoDB.
- **Automation with Airflow**: Use of Airflow to automate the entire migration process.

## How to Run the Project
-------------------------

### Prerequisites

- Python 3.12
- Airflow 2.10.5
- Docker (to run a MongoDB container)
- MongoDB installed and running
- Access to the FI-Admin API

### Install Python Requirements

Clone this project and then run:

```pip install -r ./requirements.txt```

### Run the service

```sh run_standalone_server.sh```

This command will initialize a docker container with MongoDB and the Airflow service. Once started, the admin password will be created inside the file `standalone_admin_password.txt`.

### Index in MongoDB

Create an unique index for field `id` in your collection. The field `id` is the primary key in FI-Admin records:
```
db.collection.createIndex({ id: 1 }, { unique: true })
```

### Airflow Connection Configuration

Create two connections in Airflow Admin web page (`Admin` > `Connections`), one for FI-Admin API and the other for MongoDB.

#### FI-Admin API
- Connection Id: fiadmin
- Connection Type: HTTP
- Host: https://api.bvsalud.org/bibliographic/v1/ (check if it has changed)
- Password: Your API Key

#### MongoDB
- Connection Id: mongo
- Connection Type: MongoDB
- Host: localhost (if local development)
- Default DB: create a database and place its name here
- Username: create a user with permission to access the database and place its name here
- Password: same as above
- Port: 27017
- Extra: ```{"srv": false, "ssl": false, "allow_insecure": false}``` (if local development)