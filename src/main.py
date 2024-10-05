import json
import os

from google.auth.environment_vars import PROJECT
from google.cloud import bigquery, storage
from flask import jsonify, Config


def get_config(config_file="config.json"):
    """Loads the configuration from a JSON file."""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
            return config
    except FileNotFoundError:
        print(f"Config file {config_file} not found.")
        raise
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {config_file}")
        raise

def load_all_csv_to_bigquery(bucket_name):
    """Loads all CSV files from a Google Cloud Storage bucket into BigQuery,
    and moves them to an archive folder after successful load.

    Args:
        bucket_name (str): The name of the GCS bucket.
    """
    # Initialize BigQuery and Storage clients
    bq_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    config = get_config()

    print(f"Listing files from bucket: {bucket_name}")
    # List all files in the bucket
    blobs = bucket.list_blobs()

    # Define BigQuery dataset and table (use environment variables)
    dataset_id = 'landing' # os.getenv('BQ_DATASET')

    for blob in blobs:

        if 'archive' in blob.name.lower():
            print(f"Skipping archive files")
            continue
        # Only process files that have a .csv extension
        if blob.name.endswith('.csv') and blob.name.split('/')[0].lower() in config.keys():

            print(f"Processing file: {blob.name}")

            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            table_id = blob.name.split('/')[0].lower()
            # BigQuery load job configuration
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter=config.get(table_id)["delimiter"],
                skip_leading_rows=1,  # Adjust if your CSV contains a header row
                autodetect=True,  # Automatically detect the schema
                encoding="utf-8",
                column_name_character_map='V2'

            )

            try:
                # Load the CSV file into BigQuery
                load_job = bq_client.load_table_from_uri(gcs_uri,
                                                         f'{dataset_id}.{table_id}', job_config=job_config)
                load_job.result()  # Wait for the job to complete

                print(f"File {blob.name} successfully loaded into {dataset_id}.{table_id}")

                # After a successful load, move the file to the archive folder
                move_to_archive(storage_client, bucket_name, blob.name)

            except Exception as e:
                print(f"Error loading file {blob.name} into BigQuery: {e}")
                continue  # Continue processing other files if there's an error


def move_to_archive(storage_client, bucket_name, file_name):
    """
    Moves the processed file to an archive folder within the same bucket,
    preserving the folder structure (e.g., ing/, revolut/).

    Args:
        storage_client: The initialized Google Cloud Storage client.
        bucket_name: The name of the bucket.
        file_name: The name of the file, including its folder structure (e.g., ing/myfile.csv).
    """
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(file_name)

    # Construct the archive path, preserving the original folder structure
    archive_path = f"archive/{file_name}"

    # Copy the file to the archive folder
    bucket.copy_blob(source_blob, bucket, archive_path)
    print(f"File copied to {archive_path}")

    # Delete the original file
    source_blob.delete()
    print(f"Original file {file_name} deleted from the bucket")


def run_main(request):
    """HTTP trigger for manual or scheduled invocations.
    The request should include the bucket name to process all CSV files.

    Args:
        request (flask.Request): The HTTP request object.

    Returns:
        JSON response indicating success or failure.
    """
    request_json = request.get_json(silent=True)

    if not request_json:
        return jsonify({'error': 'Invalid request, no data provided'}), 400

    bucket_name = request_json.get('bucket')

    if not bucket_name:
        return jsonify({'error': 'Missing bucket name'}), 400

    try:
        # Call the main function to process all CSV files in the bucket
        load_all_csv_to_bigquery(bucket_name)
        return jsonify({'status': 'success', 'bucket': bucket_name}), 200
    except Exception as e:
        return jsonify({'status': 'failure', 'error': str(e)}), 500


if __name__ == '__main__':
    load_all_csv_to_bigquery("landing-bucket-3301")
