# crypto_dag

This project creates an ETL process using apache airflow to orchestrate tasks and Google Big Query to store the resulting database.

The main goal is to retrieve information regarding Bitcoin and Ethereum prices since 01/01/2021. To achieve this, the CoinGecko API will be used (https://www.coingecko.com/en/api).

This will be done using a python script. This script is very flexible, being able to adjust itself in function of the inputs (desired crypto and desired currency). In order to achieve this flexibility, it will take advantage of the fact that the time span of each coin is the same (01/01/2021 - current date). This will allow us to use the list data structure to arrange the data and load them into the dataframe. For providing flexibility, dictionaries will also be throughly used to dinamically alocate variables.

The functioning script (crypto_dag/airflow/dags/crypto_df_dag.py) will be used as a DAG in apache-airflow to schedule this data generation and load it daily at 7 AM (UTC -3) into a google big query.

Caveat: Coins that were not listed in at least one day during this time period (01/01/2021 - current date) won't work properly, since the date list won't match the others lists length. This will be adjusted in future versions.
