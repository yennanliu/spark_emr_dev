# CLI RUN EMR JOBS

### 1. Run a emr with dummy task (and terminate it when job completed/failed)

```bash
aws emr create-cluster \
    --name "Sample Spark Cluster with a single Job" \
    --instance-type m3.xlarge \
    --release-label emr-4.1.0 \
    --instance-count 1 \
    --use-default-roles \
    --applications Name=Spark \
    --steps file://step.json \
    --auto-terminate
```

### 2. Create a emr with pyspark script in s3 (and terminate it when job completed/failed)

```bash
 aws emr create-cluster --applications Name=Hadoop Name=Spark --ec2-attributes '{"InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-ab8f8ee0","EmrManagedSlaveSecurityGroup":"sg-0a78143ccac876690","EmrManagedMasterSecurityGroup":"sg-03d310bc88ddb1633"}' --release-label emr-5.28.0 --log-uri 's3n://aws-logs-437885434504-us-west-2/elasticmapreduce/' --steps '[{"Args":["spark-submit","--deploy-mode","cluster","s3://etl-spark-bucket/pyspark_script/spark_helloworld.py"],"Type":"CUSTOM_JAR","ActionOnFailure":"TERMINATE_CLUSTER","Jar":"command-runner.jar","Properties":"","Name":"Spark 應用程式"}]' --instance-groups '[{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m3.xlarge","Name":"Master Instance Group"},{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m3.xlarge","Name":"Core Instance Group"}]' --configurations '[{"Classification":"spark","Properties":{}}]' --auto-terminate --service-role EMR_DefaultRole --enable-debugging --name 'yen-emr-10' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-west-2
 ```

### 3.Submit a job to created emr ( emr id = j-ON9Z8VHKC8FD for example)

```bash
bash 
aws emr add-steps --cluster-id j-ON9Z8VHKC8FD \
    --steps Name=Spark,Jar=s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar,Args=[/home/hadoop/spark/bin/spark-submit,--deploy-mode,cluster,s3://etl-spark-bucket/pyspark_script/spark_helloworld.py],ActionOnFailure=CONTINUE

```

# CLI RUN spark-submit JOB LOCAL 

### 1. run spark hello world job

```bash

spark-submit --class EmrHelloworld.emr_helloworld spark_emr_dev/target/scala-2.11/spark_emr_dev-assembly-1.0.jar
```