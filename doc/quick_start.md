# EMR QUICK START COMMANDS 

### Run a dummy task with emr  (and terminate it when job completed/failed)

```bash
aws emr create-cluster \
    --name "Sample Spark Cluster with a single Job" \
    --instance-type m3.xlarge \
    --release-label emr-4.1.0 \
    --instance-count 1 \
    --use-default-roles \
    --applications Name=Spark \
    --steps file://task_step/step.json \
    --auto-terminate
```

### Submit a job to created emr ( emr id = j-ON9Z8VHKC8FD for example)

```bash
bash 
aws emr add-steps --cluster-id j-ON9Z8VHKC8FD \
    --steps Name=Spark,Jar=s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=[/home/hadoop/spark/bin/spark-submit,--deploy-mode,cluster,s3://etl-spark-bucket/pyspark_script/spark_helloworld.py],ActionOnFailure=CONTINUE

```