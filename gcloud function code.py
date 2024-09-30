#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import functions_framework
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    # Extract information from the event
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]  # This is the uploaded file's name
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    # Call the function to load the file to BigQuery
    load_bq(name)

# Function to load the CSV file to BigQuery
def load_bq(filename):
    # Create a BigQuery client
    client = bigquery.Client()

    # Define the BigQuery dataset and table
    dataset = 'sales'  # Replace with your dataset name
    table = 'orders'   # Replace with your table name

    # Reference to the table
    table_ref = client.dataset(dataset).table(table)

    # Configure the load job
    job_config = LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the header row
    job_config.autodetect = True  # Let BigQuery auto-detect the schema

    # The GCS URI (link to the uploaded file)
    uri = f'gs://salesdatapipeline_bucket/{filename}'  # Replace with your bucket name

    # Load the file from GCS to BigQuery
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    # Print how many rows were loaded
    print(f"{load_job.output_rows} rows loaded into {table} table.")

