#!/bin/bash
 
time mpirun -np 48  -machinefile  ~/hostfile /home/ec2-user/kmeans/mpi/mpi_main -i ~/input-mpi/random_1000000points.csv -o -n 50000 -d #2>&1 | tee -a run.log
time mpirun -np 48  -machinefile  ~/hostfile /home/ec2-user/kmeans/mpi/mpi_main -i ~/input-mpi/random_10000000points.csv -o -n 5000 -d #2>&1 | tee -a run.log
time mpirun -np 48  -machinefile  ~/hostfile /home/ec2-user/kmeans/mpi/mpi_main -i ~/input-mpi/random_100000000points.csv -o -n 500 -d #2>&1 | tee -a run.log
