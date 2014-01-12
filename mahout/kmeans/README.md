# Mahout Kmeans Test

## Setup
Add HADOOP_CONF_DIR
export HADOOP_CONF_CIR=/usr/local/Cellar/hadoop/1.2.1/libexec/conf
export CLASSPATH=$CLASSPATH:$HADOOP_CONF_DIR


## Run
To upload files to HDFS:
java -jar kmeans-1.0-SNAPSHOT.jar <path-to-input-file>



## Cleanup

bin/hadoop fs -rmr output kmeans