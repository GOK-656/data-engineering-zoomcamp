### Data Proc
To insert data into BigQuery, run this command. Remember to substitute the directory to your own. 

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dezoomcamp_project_dtc-de-404803/code/to_bigquery.py \
    -- \
        --input_records=gs://dezoomcamp_project_dtc-de-404803/records/ \
        --input_loc=gs://dezoomcamp_project_dtc-de-404803/loc/ \
        --output=project_data.covid
```
