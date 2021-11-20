# Using Spanner Emulator for Benchbase

You can start the emulator using `up.sh`.  This just simply launches the following command...`docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator`.  To be truly useful the emulator must be configured using `gcloud`.  I used the following commands to get the emulator properly configured after starting.

1) Create a new `config` called `emulator-config` with the following properties:
```
gcloud config configurations create emulator-config
gcloud config set auth/disable_credentials true
gcloud config set api_endpoint_overrides/spanner http://localhost:9020/
gcloud config set project benchbase-project
```

2) Create a Spanner instance called `benchbase-instance` inside the emulator using the new `config` called `emulator-config`
```
gcloud spanner instances create benchbase-instance --config=emulator-config --description="Benchbase Emulator Instance" --nodes=1
```
5) Create a new Spanner database called `benchbase` in the new `benchbase-instance`.
```
gcloud spanner databases create benchbase --instance benchbase-instance
```

Once you are done you can clean up the resources you've created...
1) This will delete the Spanner instance.
```
gcloud spanner instances delete benchbase-instance
```
2) Activate your default `configuation`.
```
gcloud config configurations activate default
```
3) Delete your `emulator-config`.
```
gcloud config configurations delete emulator-config
```
4) Bring down the Emulator docker image.
