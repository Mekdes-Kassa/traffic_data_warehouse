# Data-warehouse-tech-stack-with-MySQL-DBT-Airflow
creating a scalable data warehouse that will host the vehicle trajectory data extracted by analysing footage taken by swarm drones and static roadside cameras

Data Warehouse for City Traffic Data -

This repository contains the necessary information and resources to set up a scalable data warehouse for city traffic data. The data warehouse is designed to store and analyze vehicle trajectory data collected by swarm UAVs (drones) and static roadside cameras. The data warehouse uses a tech stack comprising PostgreSQL as the database, DBT for Extract Load Transform (ELT) workflows, and Airflow for workflow orchestration. Business Need

The objective of this project is to assist the city traffic department in improving traffic flow and undertaking various data-driven projects by leveraging the vehicle trajectory data collected from multiple locations in the city. This data will provide critical insights and intelligence to optimize traffic management and support future initiatives.
Data Warehouse Architecture

The data warehouse follows an ELT (Extract Load Transform) framework, which allows analytic engineers to set up transformation workflows as needed. The architecture consists of the following components:

Data Sources: The raw data is collected from swarm UAVs and static roadside cameras, capturing vehicle trajectory information. These data sources feed into the data warehouse for further processing and analysis.

PostgreSQL Database: The data warehouse utilizes either PostgreSQL as the relational database management system (RDBMS) for storing and querying the collected data. Choose one of these databases based on your specific requirements and preferences.

DBT (Data Build Tool): DBT is a powerful tool used for Extract Load Transform (ELT) workflows. It allows you to define transformations and build data models on top of your raw data. With DBT, you can create reusable and maintainable SQL transformations, manage dependencies between tables, and ensure data consistency.

Airflow: Airflow is an open-source platform used for workflow orchestration and scheduling. It provides a rich set of features for managing and automating data pipelines. In this project, Airflow is used to schedule and execute DBT jobs, ensuring that the data transformations are performed on a regular basis.

Repository Structure

This repository is organized as follows:

/Data: This directory contains sample data files representing the raw vehicle trajectory data collected from the swarm UAVs and static roadside cameras. Use this data to simulate the data ingestion process.

/dbt_data_transforming: This directory contains the DBT project, including the configuration files and SQL transformations. Customize these files to match your specific requirements and data models.

dags: This directory contains the Airflow DAGs (Directed Acyclic Graphs) that define the data pipeline workflows. Customize these DAGs to schedule and execute the DBT jobs based on your desired frequency.

README.md: This file provides an overview of the project, its objectives, and the repository's structure.


