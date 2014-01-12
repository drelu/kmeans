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

    java -jar /data/kmeans/mahout/kmeans/target/kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar /data/kmeans/data/1GB.csv 

Run Mahout Kmeans:

     export CLASSPATH=$CLASSPATH:/home/hadoop/conf/
     export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/conf/
     export HADOOP_CONF_DIR=/home/hadoop/conf/
     java -jar /data/kmeans/mahout/kmeans/target/kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar /data/kmeans/data/1GB.csv 


## TODO

*Spark
*Twister