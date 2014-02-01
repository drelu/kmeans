# KMEANS Spark Implementation

Set environment

    export SPARK_HOME=/usr/local/spark-0.8.1-incubating-bin-hadoop2
    export SPARK_CLASSPATH=`pwd`/target/scala-2.9.3/kmeans-spark-java_2.9.3-1.0.jar


Build

    sbt package

    
Run
    
    spark-class com.drelu.KMeans /usr/local/spark-0.8.1-incubating-bin-hadoop2/ spark://<hostname>:7077 `pwd`/target/scala-2.9.3/kmeans-spark-java_2.9.3-1.0-one-jar.jar random_1000000points.csv 500
    