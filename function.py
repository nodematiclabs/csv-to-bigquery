import os
import logging

import functions_framework

from google.cloud import bigquery
from google.cloud import storage

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def create_bigquery_table(cloud_event):
    """Background Cloud Function to be triggered by Cloud Storage.
    This function gets triggered when a file is uploaded to the specified Google Cloud Storage Bucket.
    
    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    """
    data = cloud_event.data

    # Configurations
    bucket_name = data['bucket']
    file_name = data['name']
    dataset_name = 'csv-to-bigquery-demo'
    table_name = 'partner_orders'
    
    # Initialize Google Cloud clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()
    
    # Construct the URI for the uploaded CSV
    uri = f'gs://{bucket_name}/{file_name}'
    
    # Construct a BigQuery client object.
    client = bigquery.Client()
    
    # Set table and dataset reference
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    
    # Define BigQuery job config for loading the CSV
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True  # Auto detect schema

    # Create a load job to create the table and load data
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  

    # Wait for the load job to complete
    load_job.result()

    logging.info(f'Table {table_name} successfully created and data loaded from {uri}.')