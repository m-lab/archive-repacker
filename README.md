# archive-repacker

Process and repackage existing archives.

## Node pool configuration

The archive-repacker services run in the data-processing cluster. So these
services do not interfere with other parts of the data pipeline, a separate
node-pool is dedicated to the archive-repacker.

```sh
gcloud --project=${PROJECT} container node-pools create archive-repacker \
  --cluster=data-processing --region=us-east1 --enable-autoscaling \
  --max-nodes=9 --min-nodes=0 --total-min-nodes=0 --num-nodes=0 \
  --node-labels=archiver=true --enable-autorepair --enable-autoupgrade \
  --machine-type=c2-standard-8 --scopes=cloud-platform
```
