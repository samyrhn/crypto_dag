# crypto_dag

This project creates an ETL process using apache airflow to orchestrate tasks and Google Big Query to store the resulting database.

The main goal is to retrieve information regarding Bitcoin and Ethereum prices since 01/01/2021. To achieve this, the CoinGecko API will be used (https://www.coingecko.com/en/api).

This will be done using a python script. This script is very flexible, being able to adjust itself in function of the inputs (desired crypto and desired currency). In order to achieve this flexibility, it will take advantage of the fact that the time span of each coin is the same (01/01/2021 - current date). This will allow us to use the list data structure to arrange the data and load them into the dataframe. For providing flexibility, dictionaries will also be throughly used to dinamically alocate variables.

The functioning script (crypto_dag/airflow/dags/crypto_df_dag.py) will be used as a DAG in apache-airflow to schedule this data generation and load it daily at 7 AM (UTC -3) into a google big query.

Caveat: Coins that were not listed in at least one day during this time period (01/01/2021 - current date) won't work properly, since the date list won't match the others lists length. This will be adjusted in future versions.

Inside the dag folder you can find 2 python files. The first one, "crypto_dag_df.py" is the one that accomplishes the tasks described above.
The second one "dag_in_progress.py" is an altered version of the first one. It was created to solve some one main issue regarding the first one. This second script should be able to detect that an existing table already exists in the GBQ and append the missing data into it, instead of replacing it with a whole new table. However, one property of CoinGecko's API is that it adjusts automatically its date granularity in function of the date range, which is causing some issues to implement this change. As the name says, this dag is still in development.
