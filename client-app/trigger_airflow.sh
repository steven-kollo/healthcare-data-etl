#!/bin/bash
sudo gcloud composer environments run healthcare --location us-central1 dags trigger -- read_bucket_file