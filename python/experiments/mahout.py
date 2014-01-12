import sys
import os
import math
import time
import logging
import uuid
import datetime
import copy
import glob
import traceback

RESULT_DIR="results"
RESULT_FILE_PREFIX="mahout-"
HEADER = ("Run", "Input", "Time Type", "Time")
HEADER_CSV = ("%s;%s;%s;%s\n"%HEADER)
HEADER_TAB = ("%s\t%s\t\t%s\t%s\n"%HEADER)
NUMBER_REPEATS=1
INPUT_DATA_PATH="/data/input/"


# Commands for the individual steps
MAHOUT_VECTOR_CONVERSION_CMD="hadoop -jar /data/kmeans/mahout/kmeans/target/kmeans-1.0-SNAPSHOT-jar-with-dependencies.jar"

HDFS_PUT_CMD="hadoop fs -put"

MAHOUT_KMEANS_CMD="/data/bin/mahout-distribution-0.8/bin/mahout kmeans --input /user/hadoop/kmeans/points/ --output /user/hadoop/kmeans/output --clusters /user/hadoop/kmeans/clusters --numClusters 5 --overwrite --outlierThreshold 0 --distanceMeasure org.apache.mahout.common.distance.EuclideanDistanceMeasure --maxIter 10 "


""" Test Job Submission via BigJob """
if __name__ == "__main__":
    try:
        os.mkdir(RESULT_DIR)
    except:
        pass

        
    d =datetime.datetime.now()
    result_filename = RESULT_FILE_PREFIX + d.strftime("%Y%m%d-%H%M%S") + ".csv"
    f = open(os.path.join(RESULT_DIR, result_filename), "w")
    f.write(HEADER_CSV)
    for i in range(0, NUMBER_REPEATS):
        
        for f in os.listdir(INPUT_DATA_PATH):
            print "Run kmeans with: " + str(f)
            try:
                start = time.time()
                os.system(MAHOUT_VECTOR_CONVERSION + " " + f)
                end_conversion = time.time()

                f.writelines(result)
                f.flush()
            
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                print "Run " +str(i) + " failed: " + str(exc_value)
                print "*** print_tb:"
                traceback.print_tb(exc_traceback, limit=1, file=sys.stderr)
                print "*** print_exception:"
                traceback.print_exception(exc_type, exc_value, exc_traceback,
                                           limit=2, file=sys.stderr)
            time.sleep(10)

    f.close()
    print("Finished run")
