export SPARK_CLASSPATH=target/scala-2.10/kmeans-spark-java_2.10-1.0.jar 
spark-class edu.indiana.salsa.kmeans.SeqDistanceCalcTester


spark-class com.drelu.KMeans /home/ec2-user/spark-0.9.0-incubating-bin-hadoop2/ local[1] target/scala-2.10/kmeans-spark-java_2.10-1.0.jar file:///data/input-judy/random_1000000points_50000clusters.csv 50000
