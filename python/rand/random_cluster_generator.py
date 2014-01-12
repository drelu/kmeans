import numpy as np

# Size of output file will be NUMBER_CLUSTERS * NUMBER_POINTS_PER_CLUSTER * 32 Byte
NUMBER_CLUSTERS=5
NUMBER_POINTS_PER_CLUSTER=6710886.4


OUTPUT_FILE="random_data.csv"


np.random.seed(seed=1234567)     

centers = 10 * np.random.rand(NUMBER_CLUSTERS, 2)
print "Centers: " + str(centers)
random_data = []

f = open(OUTPUT_FILE, "w")

for i in range(0, NUMBER_CLUSTERS):
    data = 10*np.random.randn(NUMBER_POINTS_PER_CLUSTER, 2) + centers[i]
    
    print "Cluster %d: %s"%(i,str(data))
    for d in data:
        f.write(str(d[0]) +"," + str(d[1]) +"," + str(i) + "\n")
    
    
f.close()