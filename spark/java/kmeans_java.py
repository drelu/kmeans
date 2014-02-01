"""
KMeans clustering of Wikipedia pages using Java/Spark.

"""
import os
import sys
import numpy as np
import time
import datetime


#NUMBER_ITERATIONS=1
#FILES=["/data/kmeans/spark/java/random_1000000points.csv"] 
FILES=["/data/input-judy/random_100000000points_500clusters.csv",
       "/data/input-judy/random_1000000points_50000clusters.csv",
       "/data/input-judy/random_10000000points_5000clusters.csv"    
       ] 
#FILES=[
#       "/data/input-judy/random_1000000points_50000clusters.csv",
#       "/data/input-judy/random_10000000points_5000clusters.csv"    
#       ] 
NUMBER_CLUSTERS=[500, 50000, 5000]
HEADER = ("Run", "File", "Timestamp", "Number_Points", "Number_Clusters" "Time_Type", "Time")
HEADER_CSV = ("%s;%s;%s;%s;%s;%s;\n"%HEADER)
RESULT_DIR="results"
RESULT_FILE_PREFIX="kmeans-spark-java"
HADOOP="/root/ephemeral-hdfs/bin/hadoop"
HDFS_WORKING_DIR="/user/root/spark-kmeans"
SPARK_HOME="/root/spark/"
KMEANS_JAR=os.path.join(os.getcwd(), "target/scala-2.9.3/kmeans-spark-java_2.9.3-1.0.jar")
NUMBER_REPEATS=5


if __name__ == "__main__":
    #master = open("/root/spark-ec2/cluster-url").read().strip()
    #masterHostname = open("/root/spark-ec2/masters").read().strip()
    #SparkContext.setSystemProperty('spark.executor.memory', '2g')
    #convergeDist = 1e-5
    master = open("/root/spark-ec2/cluster-url").read().strip()
    masterHostname = open("/root/spark-ec2/masters").read().strip()
    
    os.system(HADOOP + " fs -mkdir " + HDFS_WORKING_DIR)
     
    d =datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    try:
        os.makedirs(RESULT_DIR)
    except:
        pass
    time_log = []
    for repeat in range(0, NUMBER_REPEATS):
      for idx, file in enumerate(FILES):
          start = time.time()
          os.system(HADOOP + " fs -put " + file + " " + HDFS_WORKING_DIR)
          hdfs_upload = time.time()-start
          spark_start = time.time() 
          count = file[file.find("_")+1:file.rfind("points")]
          K = NUMBER_CLUSTERS[idx]
          print "Index: " + str(idx) + " File: " + str(file) + " Points: " + str(count) + " Clusters: " + str(K) 
          os.environ["SPARK_CLASSPATH"]=KMEANS_JAR
          cmd = (os.path.join(SPARK_HOME, "spark-class") + 
          " com.drelu.KMeans " + SPARK_HOME + " " + 
          master + " " +
          KMEANS_JAR + " " +
          os.path.join(HDFS_WORKING_DIR, os.path.basename(file)) + " " +
          str(K))
          print "command: " + cmd
          os.system(cmd)
          run_time = time.time() - spark_start
          result_tuple = (repeat, file, datetime.datetime.today().isoformat(), count, K)
          hdfs_upload_tuple = result_tuple + ("Upload Time", str(hdfs_upload))
          time_log.append("%s;%s;%s;%s;%s;%s;%s\n"%(hdfs_upload_tuple))
          load_time_tuple = result_tuple + ("Run Time", str(run_time))
          time_log.append("%s;%s;%s;%s;%s;%s;%s\n"%(load_time_tuple))

      f = open(os.path.join(RESULT_DIR, result_filename), "w")
      f.write(HEADER_CSV)
      for i in time_log:
          f.write(i)
      f.close()
    
    
    #print "Clusters with some articles"
    #numArticles = 10
    #for i in range(0, len(centroids)):
    #  samples = data.filter(lambda (x,y) : closestPoint(y, centroids) == i).take(numArticles)
    #  for (name, features) in samples:
    #    print name
    #  print " "

