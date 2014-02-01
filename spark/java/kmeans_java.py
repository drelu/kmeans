"""
KMeans clustering of Wikipedia pages using Java/Spark.

"""
import os
import sys
import numpy as np
import time
import datetime


NUMBER_ITERATIONS=1
FILES=["/data/kmeans/spark/java/random_1000000points.csv"] 
NUMBER_CLUSTERS=[1, 1, 5000, 50000]
HEADER = ("Run", "File", "Timestamp", "Number_Points", "Number_Clusters" "Time_Type", "Time")
HEADER_CSV = ("%s;%s;%s;%s;%s;%s;\n"%HEADER)
RESULT_DIR="results"
RESULT_FILE_PREFIX="kmeans-spark-java"
HADOOP="/root/ephemeral-hdfs/bin/hadoop"
HDFS_WORKING_DIR="/user/root/spark-kmeans"
SPARK_HOME="/usr/local/spark-0.8.1-incubating-bin-hadoop2/"
KMEANS_JAR=os.path.join(os.getcwd(), "target/scala-2.9.3/kmeans-spark-java_2.9.3-1.0.jar")



if __name__ == "__main__":
    #master = open("/root/spark-ec2/cluster-url").read().strip()
    #masterHostname = open("/root/spark-ec2/masters").read().strip()
    #SparkContext.setSystemProperty('spark.executor.memory', '2g')
    #convergeDist = 1e-5
    master = open("/root/spark-ec2/cluster-url").read().strip()
    masterHostname = open("/root/spark-ec2/masters").read().strip()
    
    os.system(HADOOP + " fs -mkdir " + HDFS_WORKING_DIR)
    #sc = SparkContext("spark://LMUCX29607.local:7077", "PythonKMeans")
    #sc = SparkContext("local", "PythonKMeans")
    sc = SparkContext(master, "PythonKMeans")
     
    time_log = []
    for idx, file in enumerate(FILES):
        start = time.time()
        os.system(HADOOP + " fs -put " + file + " " + HDFS_WORKING_DIR)
        hdfs_upload = time.time()-start
        spark_start = time.time() 
        print "Index: " + str(idx) + " File: " + str(file)
        count = filename[filename.find("_")+1:filename.rfind("points")]
        K = NUMBER_CLUSTERS[idx]
        os.system(os.path.join(SPARK_HOME, "spark-class") + 
        " com.drelu.KMeans /usr/local/spark-0.8.1-incubating-bin-hadoop2/ " + 
        master + " " +
        KMEANS_JAR + " " +
        file + " " +
        K)
        run_time = time.time() - spark_start
        result_tuple = (0, file, datetime.datetime.today().isoformat(), count, K)
        load_time_tuple = result_tuple + ("Load Time", str(run_time))
        time_log.append("%s;%s;%s;%s;%s;%s;%s\n"%(load_time_tuple))

    d =datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    try:
        os.makedirs(RESULT_DIR)
    except:
        pass
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

