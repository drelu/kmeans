# Kmeans Experiments


## Mahout:

kmeans example from Mahout in Action


## Python:

Random Data generator


## Amazon EMR

EBS Volume with Experiments project and tests data

Format (initially)
    
    sudo mkfs -t ext4 /dev/sdf


Mount EBS
    
    mkdir /data
    mount /dev/sdf /data


Upload Data to HDFS:

    export CLASSPATH=$CLASSPATH:/home/hadoop/conf/
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/conf/
    export HADOOP_CONF_DIR=/home/hadoop/conf/

    hadoop -jar /data/kmeans/mahout/kmeans/target/kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar /data/kmeans/data/1GB.csv 

Run Mahout Kmeans:

   /data/bin/mahout-distribution-0.8/bin/mahout kmeans --input /user/hadoop/kmeans/points/ --output /user/hadoop/kmeans/output --clusters /user/hadoop/kmeans/clusters --numClusters 5 --overwrite --outlierThreshold 0 --distanceMeasure org.apache.mahout.common.distance.EuclideanDistanceMeasure --maxIter 10  


## TODO

*Spark
*Twister

