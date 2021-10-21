#!/bin/bash

docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

#gcloud config configurations create emulator
#gcloud config set auth/disable_credentials true
#gcloud config set project test-project

## the emulator must be up for these to run
#gcloud config set api_endpoint_overrides/spanner http://localhost:9020/

#gcloud spanner instances create test-instance --config=emulator-config --description="Test Instance" --nodes=1

#gcloud spanner databases create test-db --instance test-instance