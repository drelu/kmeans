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
import threading
from pilot import PilotComputeService, PilotDataService, ComputeDataService, ComputeDataServiceDecentral, State, DataUnit
from bigjob import logger 
from email.header import Header

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


COORDINATION_URL="redis://ILikeBigJob_wITH-REdIS@gw68.quarry.iu.teragrid.org:6379"
#COORDINATION_URL="redis://Oily9tourSorenavyvault@redis01.tacc.utexas.edu:6379"

JOBS = {
     "OSG-SSH":  { 
                  #"pilot_compute_url":["condor+ssh://gw68.quarry.iu.teragrid.org?WhenToTransferOutput=ON_EXIT&should_transfer_files=YES&notification=Always"],
                  "pilot_compute_url":["condor://gw68.quarry.iu.teragrid.org?WhenToTransferOutput=ON_EXIT&should_transfer_files=YES&notification=Always"],
                  "pilot_data_url":"ssh://luckow@engage-submit3.renci.org/scratch3/luckow/pilot-data",
                  #"data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-4b373290-4a55-11e2-9481-4fff24ec45d6",
                  "data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-57a22c0e-5cfe-11e2-853f-39b1030b5855",
                  "working_directory" : [os.path.join(os.getcwd(), "agent")],
                  "number_of_processes" : [1],
                  "processes_per_node": [1],
                  "number_pilots": 8,
                  "number_subjobs" : 8,
                  "queue": "normal",
                  "project": "TG-MCB090174",
                  "walltime": ["120"]
                } ,
     "TR":  { 
                  "pilot_compute_url":["pbs+ssh://luckow@trestles.sdsc.edu"],
                  "pilot_data_url":"ssh://tg804093@login2.ls4.tacc.utexas.edu/work/01131/tg804093/pilot-data",
                  # only ref data
                  #"data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-98914f58-4c76-11e2-8638-f1559c9c2a8a",
                  # ref data + read files
                  "data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-c9f3e7ce-9dfa-11e2-9454-5254002792d2",
                  #"data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-274a3ce6-5c8f-11e2-bf94-673131439a47", # ref + 256 read + OSG
                  "working_directory" : ["/oasis/scratch/trestles/luckow/646368.trestles-fe1.sdsc.edu"],
                  "number_of_processes" : [16],
                  "processes_per_node": [16],
                  "number_pilots": 1,
                  "number_subjobs" : 1024,
                  "queue": ["normal", "normal"],
                  "project": ["TG-MCB090174"],
                  "walltime": ["10", "10"] 
                } ,
     "SP-SSH":  { 
                  "pilot_compute_url":["slurm+ssh://tg804093@login1.stampede.tacc.utexas.edu"],
                  "pilot_data_url":"ssh://tg804093@login1.stampede.tacc.utexas.edu/work/01131/tg804093/pilot-data",
                  # ref data + 1 read file
                  #"data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-a5d732dc-9400-11e2-bb25-5254002792d2",
                  "data_unit_url":"redis://gw68.quarry.iu.teragrid.org/bigdata:du-ebb8e76c-9cfe-11e2-b226-5254002792d2",
                  "working_directory" : ["/scratch/01131/tg804093/pilot-compute", "/work/01131/tg804093/pilot-compute"],
                  "number_of_processes" : [16],
                  "processes_per_node": [16],
                  "number_pilots": 1,
                  "number_subjobs" : 1024,
                  "queue": ["normal", "normal"],
                  "project": ["TG-MCB090174"],
                  "walltime": ["10", "120"] 
                } ,
        "YARN":  { 
                  "pilot_compute_url":["yarn://localhost:8032?fs=hdfs://localhost:9000/"],
                  "pilot_data_url":"ssh://localhost/pilot-data-" + str(uuid.uuid1()),
                  "number_of_processes" : [1],
                  "processes_per_node": [1],
                  "number_pilots": 1,
                  "number_subjobs" : 1,
                  "queue": ["normal", "short"],
                  "project": ["", ""],
                  "walltime": ["60", "60"] 
                } ,       
        
         "AWS":  { 
                  "pilot_compute_url":["ec2+ssh://aws.amazon.com/"],
                  "pilot_data_url":"s3://pilot-data-" + str(uuid.uuid1()),
                  "access_key_id": "AKIAJPGNDJRYIG5LIEUA", # in ~/.boto
                  "secret_access_key":"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", # in ~/.boto
                  "vm_id":"ami-6da11704",
                  "vm_ssh_username":"ubuntu",
                  "vm_ssh_keyname":"radical",
                  "vm_ssh_keyfile":"radical.pem",
                  "vm_type":"t1.micro",
                  "vm_location":"",
                  "working_directory" : ["/home/ubuntu"],
                  "number_of_processes" : [1, 256],
                  "processes_per_node": [1, 16],
                  "number_pilots": 1,
                  "number_subjobs" : 1,
                  "queue": ["normal", "short"],
                  "project": ["TG-MCB090174", "TG-MCB090174"],
                  "walltime": ["479", "479"] 
                } ,
}




JOB_TYPES = ["YARN"]
#JOB_TYPES = ["SP-SSH"]
#JOB_TYPES = ["XSEDE-SSH"]
RESULT_DIR="results"
RESULT_FILE_PREFIX="pilotstartup-"
HEADER = ("Run", "PC", "PD", "Number jobs", "Read File Size", "Timestamp", "Time Type", "Time")
HEADER_CSV = ("%s;%s;%s;%s;%s;%s;%s;%s\n"%HEADER)
HEADER_TAB = ("%s\t%s\t\t%s\t%s\t%s\t%s\t%s\t%s"%HEADER)
NUMBER_REPEATS=10
FILEPATH=os.path.join(os.getcwd(), "data/file.txt")
BWA_DATA_DIR="/home/luckow/data/bwa"
SIZE=1024
CU_NUMBER_OF_CORES=2

pilot_compute_service = PilotComputeService(coordination_url=COORDINATION_URL)

def start_pilot(job, pilot):
    print("**Start Pilot: %d at %s"%(pilot, job["pilot_compute_url"][pilot]) )
    pilot_compute_description = {
                             "service_url": job["pilot_compute_url"][pilot],
                             "number_of_processes": job["number_of_processes"][pilot],
                             "queue":job["queue"][pilot],
                             "walltime":job["walltime"][pilot],
                             "access_key_id": "AKIAJPGNDJRYIG5LIEUA", # in ~/.boto
                             "secret_access_key":"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", # in ~/.boto
                             "vm_id":"ami-d0f89fb9",
                             "vm_ssh_username":"ubuntu",
                             "vm_ssh_keyname":"gw68",
                             "vm_ssh_keyfile":"gw68",
                             "vm_type":"t1.micro",
                             "vm_location":""
                            }   
    print pilot_compute_description

    if job.has_key("working_directory"):
        pilot_compute_description["working_directory"]=job["working_directory"][pilot]
    if job.has_key("project") and job["project"][pilot]!=None:
        print "Project: %s"%(str(job["project"][pilot]))
        pilot_compute_description["project"]=job["project"][pilot]
    print(str(pilot_compute_description)) 
    pj = pilot_compute_service.create_pilot(pilot_compute_description=pilot_compute_description)
    return pj

def test_pilotdata(job, run_id, size=1):
    start_time = time.time()

    # Logging information 
    time_log = []
    number_jobs = job["number_subjobs"]
    lrms_compute = str(job["pilot_compute_url"])
    lrms_data = str(job["pilot_data_url"])
    
    result_tuple = (run_id, lrms_compute, lrms_data, number_jobs, size, datetime.datetime.today().isoformat(),str(job["number_pilots"]))
            
    pilotjobs = []
    logger.debug("*******************************************************************************************")
    logger.debug("Start %d pilots."%(job["number_pilots"]*len(job["pilot_compute_url"])))
    for i in range(0, job["number_pilots"]):
        for pilot in range(0, len(job["pilot_compute_url"])):
            pj = start_pilot(job, pilot)
            submission_time = time.time() - start_time
            pilot_startup_begin = time.time()
            submission_time_tuple = result_tuple + ("Pilot Submission Time", str(submission_time))
            time_log.append("%s;%s;%s;%s;%s;%s;%s;%s;%s\n"%(submission_time_tuple))  
            pj.wait()
            pilot_startup_time = time.time() - pilot_startup_begin
            pilot_startup_time_tuple = result_tuple + ("Pilot Startup Time", str(pilot_startup_time))
            time_log.append("%s;%s;%s;%s;%s;%s;%s;%s;%s\n"%(pilot_startup_time_tuple))
    
    
    logger.debug("Started %d pilots"%len(pilotjobs))
    
#     pj_startup_threads=[]
#     for i in range(0, job["number_pilots"]):
#         for pilot in range(0, len(job["pilot_compute_url"])):
#             t=threading.Thread(target=start_pilot, args=(job, pilot))
#             t.start()
#             pj_startup_threads.append(t)
#                             
#     for t in pj_startup_threads:
#         t.join()

    logger.debug("Started %d pilots"%len(pilotjobs))
    #result_tuple = (run_id, lrms_compute, lrms_data, number_jobs, size, datetime.datetime.today().isoformat(),str(job["number_pilots"]))
    #all_pilots_active = time.time() - start_time
    #all_pilots_active_tuple = result_tuple+ ("Pilot Submission Time", str(all_pilots_active))
    #time_log.append("%s;%s;%s;%s;%s;%s;%s;%s;%s\n"%(all_pilots_active_tuple))       
        
    logger.info("Terminate Pilot Compute/Data Service")
    pilot_compute_service.cancel()
    return time_log
    
    
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
        for t in JOB_TYPES:
            jobs_copy = copy.deepcopy(JOBS)
            #for np in [1,2,4,8,16,32,48]:
            #for np in [48]:
            #for np in [1,2,4,8,16,32]:
            try:
                jobs_copy[t]["number_pilots"]=1                    
                print "Run with %d pilots"%int(jobs_copy[t]["number_pilots"])
                result = test_pilotdata(jobs_copy[t], run_id=i, size=SIZE)
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
            time.sleep(5)

    f.close()
    print("Finished run")
    os.system("ssh tg804093@login2.ls4.tacc.utexas.edu  pkill -u tg804093")
    #os.remove(FILEPATH)
