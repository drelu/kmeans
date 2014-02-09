#!/bin/bash

time mpirun -np 12 -machinefile  ~/hostfile /home/ec2-user/kmeans/mpi/mpi_main -i ~/input-mpi/random_1000000points.csv -o -n 50000
#time mpirun -np 24 -machinefile  ~/hostfile ../mpi_main -i ~/input-mpi/random_10000000points.csv -o -n 5000
#time mpirun -np 24 -machinefile  ~/hostfile ../mpi_main -i ~/input-mpi/random_100000000points.csv -o -n 500
