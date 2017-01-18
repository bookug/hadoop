#! /bin/bash
export YARN_CONF_DIR=/usr/lib/cloud/hadoop/hadoop-2.2.0/etc/hadoop
export SPARK_JAR=/usr/lib/cloud/spark/spark-0.9.0-incubating-bin-hadoop2/assembly/target/scala-2.10/spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0.jar

/usr/lib/cloud/spark/spark-0.9.0-incubating-bin-hadoop2/bin/spark-class org.apache.spark.deploy.yarn.Client \
--jar mysparktest.jar \
--class mysparktest.jar \
--args yarn-standalone \
--args /user/zhangdeyang/testspark \
--num-workers 3 \
--master-memory 485m \
--worker-memory 485m \
--worker-cores 2
