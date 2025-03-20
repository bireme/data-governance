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
- MongoDB installed and running
- Access to the FI-Admin API