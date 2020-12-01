# Calculating Moving Average

This code is an exercise to calculate the moving average for N days.
The datasource used is the [Historical Stock Price from 1970 - 2018](https://www.kaggle.com/ehallmar/daily-historical-stock-prices-1970-2018?select=historical_stocks.csv)

## Setup Environment
The exercise was executed on Google Cloud Platform; therefore the following intructions are based on Cloud Storage, Bigquery and Dataproc.

### Load data
Download the data and upload the file **historical_stock_prices.csv** into a GCS Bucket.

### BigQuery
Create a table with the following schema
```
{
    "mode": "NULLABLE",
    "name": "ticker",
    "type": "STRING"
    },
    {
    "mode": "NULLABLE",
    "name": "close",
    "type": "FLOAT"
    },
    {
    "mode": "NULLABLE",
    "name": "date",
    "type": "DATE"
    },
    {
    "mode": "NULLABLE",
    "name": "moving_average",
    "type": "FLOAT"
}
```
And set a **monthly** partition with the field **date**

### Dataproc
Create a Dataproc cluster, in my case I used the following configuration
```
Master instances: 1
Master Machine Type: n1-standard-2
Node instances: 2
Node Machine Type: n1-standard-2
Image Version: 1.5.23-debian10
```
## Execute
1. Clone this repo
2. Open Cloud Shell
3. Upload the file movingAverage.py
4. Execute the following command
```
gcloud dataproc jobs submit pyspark movingAverage.py \
    --cluster=[Cluster name] \
    --region=[Region] \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    -- [bucket name for temp data] \
    [gs://path/to/file.csv] \
    [projectid:dataset.table] \
    --days=7
```
The flags **--days** is optional and it indicates the days used for the moving average, default is 7.

## Report example
https://datastudio.google.com/reporting/db611e66-956f-4f6d-baa5-d7482d1d5fe3

