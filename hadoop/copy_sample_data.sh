#!/bin/sh

aws s3  cp --recursive s3://us-west-2.elasticmapreduce.samples/cloudfront/data/ s3://cloudfront-hadoop/data/

aws s3  cp --recursive s3://us-west-2.elasticmapreduce.samples/cloudfront/code/  s3://cloudfront-hadoop/code/
