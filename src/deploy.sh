gcloud functions deploy process_all_csv_files \
--runtime python311 \
--trigger-http \
--entry-point run_main \
--env-vars-file .env.yaml \
--memory 512MB \
--timeout 300s \
--region europe-west4 \
--allow-unauthenticated  # Allow HTTP access \
