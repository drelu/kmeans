package com.drelu;


import org.apache.hadoop.fs.shell.Count;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Lists;


import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.util.Vector;



public class KMeans {

	static int closestPoint(Vector p, List<Vector> centers) {
		int bestIndex = 0;
		double closest = Double.POSITIVE_INFINITY;
		for (int i = 0; i < centers.size(); i++) {
			System.out.println("Processing vector with a length of: " +centers.get(i).length());
			double tempDist = p.squaredDist(centers.get(i));
			if (tempDist < closest) {
				closest = tempDist;
				bestIndex = i;
			}
		}
		return bestIndex;
	}
	static Vector average(List<Vector> ps) {
		int numVectors = ps.size();
		Vector out = new Vector(ps.get(0).elements());
		for (int i = 0; i < numVectors; i++) {
			out.addInPlace(ps.get(i));
		}
		return out.divide(numVectors);
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("Args lengt: " + args.length);
		if (args.length<5){
			System.out.println("Usage: java -jar kmeans-spark-java_2.9.3-1.0 <spark_home> <spark_url> <jar_file> <hdfs_url> <num_clusters>");
			System.exit(1);
			
		}
		String sparkHome = args[0];
		String sparkUrl = args[1];
		String jarFile = args[2];
		String hdfsUrl = args[3];
		int numClusters = Integer.parseInt(args[4]);
			
		
		HashMap<String, Double> timings = new HashMap<String, Double>();
		long startTime = System.currentTimeMillis();
		Logger.getLogger("spark").setLevel(Level.WARN);
		//String sparkHome = "/root/spark";
		//String jarFile = "target/scala-2.9.3/kmeans-spark-java_2.9.3-1.0.jar";
		//String master = JavaHelpers.getSparkUrl();
		//String masterHostname = JavaHelpers.getMasterHostname();
		
		System.out.println("Spark Home: " + sparkHome + 
				" Spark URL: " + sparkUrl +
				" JAR File: " + jarFile +
				" HDFS URL: " + hdfsUrl + 
				" numCluster: " + numClusters);
		
		System.setProperty("spark.cores.max", "96");
		//System.setProperty("spark.default.parallelism", "48");
		//System.setProperty("spark.storage.memoryFraction", "0.5");
		//System.setProperty("spark.speculation" , "true");
		//System.setProperty("spark.executor.memory", "1024m");
		JavaSparkContext sc = new JavaSparkContext(sparkUrl, "JavaKMeans",
				sparkHome, jarFile);
		int K = 10;
		double convergeDist = .000001;
		int numberPartitions = 24;
		System.out.println("Using "+ numberPartitions + " partitions");
		JavaPairRDD<String, Vector> data = sc.textFile(hdfsUrl, numberPartitions).map(
						new PairFunction<String, String, Vector>() {
							public Tuple2<String, Vector> call(String in) throws Exception {
								String[] parts = in.split(",");
								return new Tuple2<String, Vector>(
										parts[0], JavaHelpers.parseVector(in));
							}
						}).cache();
		
		
		long count = data.count();
		//System.out.println("Number of records " + count);
		List<Tuple2<String, Vector>> centroidTuples = data.takeSample(false, K, 42);
		final List<Vector> centroids = Lists.newArrayList();
		for (Tuple2<String, Vector> t: centroidTuples) {
			centroids.add(t._2());
		}
		System.out.println("Done selecting initial centroids");
		long endPrepTime = System.currentTimeMillis();
		timings.put("Preparation", new Double(((endPrepTime-startTime)/1000)));
		
		double tempDist;
		for(int numIter=0; numIter<10; numIter++){
			
			long startIteration = System.currentTimeMillis();
			JavaPairRDD<Integer, Vector> closest = data.map(
					new PairFunction<Tuple2<String, Vector>, Integer, Vector>() {
						public Tuple2<Integer, Vector> call(Tuple2<String, Vector> in) throws Exception {
							return new Tuple2<Integer, Vector>(closestPoint(in._2(), centroids), in._2());
						}
					}
					);
			JavaPairRDD<Integer, List<Vector>> pointsGroup = closest.groupByKey();
			Map<Integer, Vector> newCentroids = pointsGroup.mapValues(
					new Function<List<Vector>, Vector>() {
						public Vector call(List<Vector> ps) throws Exception {
							return average(ps);
						}
					}).collectAsMap();
			//tempDist = 0.0;
			//for (int i = 0; i < K; i++) {
			//	tempDist += centroids.get(i).squaredDist(newCentroids.get(i));
			//}
			for (Map.Entry<Integer, Vector> t: newCentroids.entrySet()) {
				centroids.set(t.getKey(), t.getValue());
			}
			long endIteration = System.currentTimeMillis();
			timings.put("Iteration-" + numIter, new Double(((endIteration-startIteration)/1000)));
			System.out.println("Finished iteration");
		}
		long endTime = System.currentTimeMillis();
		timings.put("Runtime", new Double(((endTime-startTime)/1000)));
		
//		System.out.println("Cluster with some articles:");
//		int numArticles = 10;
//		for (int i = 0; i < centroids.size(); i++) {
//			final int index = i;
//			List<Tuple2<String, Vector>> samples =
//					data.filter(new Function<Tuple2<String, Vector>, Boolean>() {
//						public Boolean call(Tuple2<String, Vector> in) throws Exception {
//							return closestPoint(in._2(), centroids) == index;
//						}}).take(numArticles);
//			for(Tuple2<String, Vector> sample: samples) {
//				System.out.println(sample._1());
//			}
//			System.out.println();
//		}
		sc.stop();
		
		String results = "/tmp/results";
		System.out.println("WRITE RESULTS TO FILE: " + results);
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");  
		File theDir = new File(results);
  	    // if the directory does not exist, create it
		if (!theDir.exists()) {
		    boolean result = theDir.mkdir();  
		     if(result) {    
		       System.out.println("DIR created");  
		     }
		  }
		
		
        File file = new File(results + "/spark-"+ df.format(new Date()) +".csv");  
        if ( !file.exists() )
            file.createNewFile();
        
        PrintWriter writer = new PrintWriter(file);
        writer.println("Points, Centroids, Time_Type, Time");
        String staticFields = count + ", " + numClusters + ", ";
        for (Iterator<Entry<String, Double>> it = timings.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = (Map.Entry<String, Double>) it.next();
			String line = staticFields + entry.getKey() + ", " + entry.getValue();
			writer.println(line);
		}
        writer.close();
        
        System.exit(0);
	}
}