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

## Rolebinding for pod-reader

In order for jobs to run sequentially, the renamer must wait for the reannotator
to complete. Waiting on another job requires read access to jobs within the
cluster.

```sh
kubectl create role pod-reader --verb=get --verb=list --verb=watch \
  --resource=pods,services,deployments,jobs
kubectl create rolebinding default-pod-reader --role=pod-reader \
  --serviceaccount=default:default --namespace=default
```

You can verify the role binding was successful if the following is successful:

```sh
kubectl run k8s-wait-for --rm -it --image ghcr.io/groundnuty/k8s-wait-for:v1.6 \
    --restart Never --command -- /bin/sh -c "kubectl get services"
```
