# Assignment 3: Data pipelines with Airflow and dbt!

Welcome to my repo! This is a dbt pipeline project to construct robust data pipelines utilizing Airflow for orchestration and dbt for transformation. The assignment focused on integrating and analyzing data from two primary sources - Airbnb and the Australian Census - to extract valuable insights.


# Installation

To run the project:

    cd ./
    poetry install
    poetry shell
    cd bde3_assignment

# Usage

To create snapshots:

    dbt snapshot

To run the models:

    dbt run

To run the tests:

    dbt test
