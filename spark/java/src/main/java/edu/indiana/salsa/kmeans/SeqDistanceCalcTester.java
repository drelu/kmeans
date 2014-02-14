package edu.indiana.salsa.kmeans;
        
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.util.Vector;

public class SeqDistanceCalcTester {
	String dataInputPath;
	String centroidsInputPath;
	int numDataPoints;
	int vectorSize;
	
	public SeqDistanceCalcTester(String dataInputPath, String centroidsInputPath, int numDataPoints, int vectorSize) {
		this.dataInputPath = dataInputPath;
		this.centroidsInputPath = centroidsInputPath;
		this.numDataPoints = numDataPoints;
		this.vectorSize = vectorSize;
	}
	
	
	static int closestPoint(Vector p, List<Vector> centers) {
		int bestIndex = 0;
		double closest = Double.POSITIVE_INFINITY;
		for (int i = 0; i < centers.size(); i++) {
			//System.out.println("Processing vector with a length of: " +centers.get(i).length());
			double tempDist = p.squaredDist(centers.get(i));
			if (tempDist < closest) {
				closest = tempDist;
				bestIndex = i;
			}
		}
		return bestIndex;
	}
	
	
	public void testDistanceCalcSpark() throws Exception {
		double[][] data = readData();
		List<String> lines = readCentroids();
		int numCentroids = lines.size();
		double[][] cData = new double[numCentroids][vectorSize + 1];
		double[][] newCData = new double[numCentroids][vectorSize + 1];
		int[] cCounts = new int[numCentroids];
		int index = 0;
		index = prepareCentroids(lines, cData, index);
		System.out.println("first centroid: " + cData[0][0] + ", " + cData[0][1] + ", " + cData[0][2] + ", " + cData[0][3]);
		System.out.println("last centroid: " + cData[numCentroids - 1][0] + ", " + cData[numCentroids - 1][1] + ", " + cData[numCentroids - 1][2] 
				+ ", " + cData[numCentroids - 1][3]);
		
		System.out.println("start computation...");
		long startTime = System.currentTimeMillis();
		// run through all vectors and get the minimum distance counts
		long count = 0;
		for (int i = 0; i < numDataPoints; i++) {
			double distance = 0;
			int minCentroid = 0;
			double minDistance = Double.MAX_VALUE;

			for (int j = 0; j < numCentroids; j++) {
				distance = getEuclidean2(cData[j], data[i]);
				count++;
				if (distance < minDistance) {
					minDistance = distance;
					minCentroid = j;
				}
			}

			for (int j = 0; j < vectorSize; j++) {
				newCData[minCentroid][j] += data[i][j];
			}
			cCounts[minCentroid] += 1;
		}		
		long endTime = System.currentTimeMillis();
		System.out.println("Spark! total time taken (ms): " + (endTime - startTime) + ", number of distances computed: " + count);
		
	}
	

	public void testDistanceCalcJava() throws Exception {
		double[][] data = readData();
		List<String> lines = readCentroids();
		
		int numCentroids = lines.size();
		double[][] cData = new double[numCentroids][vectorSize + 1];
		double[][] newCData = new double[numCentroids][vectorSize + 1];
		int[] cCounts = new int[numCentroids];
		int index = 0;
		index = prepareCentroids(lines, cData, index);
		System.out.println("first centroid: " + cData[0][0] + ", " + cData[0][1] + ", " + cData[0][2] + ", " + cData[0][3]);
		System.out.println("last centroid: " + cData[numCentroids - 1][0] + ", " + cData[numCentroids - 1][1] + ", " + cData[numCentroids - 1][2] 
				+ ", " + cData[numCentroids - 1][3]);
		
		System.out.println("start computation...");
		long startTime = System.currentTimeMillis();
		// run through all vectors and get the minimum distance counts
		long count = 0;
		for (int i = 0; i < numDataPoints; i++) {
			double distance = 0;
			int minCentroid = 0;
			double minDistance = Double.MAX_VALUE;

			for (int j = 0; j < numCentroids; j++) {
				distance = getEuclidean2(cData[j], data[i]);
				count++;
				if (distance < minDistance) {
					minDistance = distance;
					minCentroid = j;
				}
			}

			for (int j = 0; j < vectorSize; j++) {
				newCData[minCentroid][j] += data[i][j];
			}
			cCounts[minCentroid] += 1;
		}		
		long endTime = System.currentTimeMillis();
		System.out.println("JAVA total time taken (ms): " + (endTime - startTime) + ", number of distances computed: " + count);
	}


	private int prepareCentroids(List<String> lines, double[][] cData, int index) {
		for (String cenLine : lines) {
			String[] cenValues = cenLine.split(" ");
			for (int j=0; j<cenValues.length; j++) {
				cData[index][j] = Double.valueOf(cenValues[j]);
			}
			index++;
		}
		return index;
	}


	private List<String> readCentroids() throws FileNotFoundException,
			IOException {
		// read centroids
		List<String> lines = new LinkedList<String>();
		BufferedReader brCen = new BufferedReader(new FileReader(centroidsInputPath));
		String line = brCen.readLine();
		while (line != null) {
			line = line.trim();
			if (line.length() > 0) {
				lines.add(line);
			}
			line = brCen.readLine();
		}
		brCen.close();
		return lines;
	}


	private double[][] readData() throws FileNotFoundException, IOException {
		// read data points
		double data[][] = new double[numDataPoints][vectorSize + 1];
		DataInputStream in = new DataInputStream(new FileInputStream(dataInputPath));
		try {
			for (int i = 0; i < numDataPoints; i++) {
				for (int j = 0; j < vectorSize; j++) {
					data[i][j] = in.readDouble();
				}
			}
		} finally {
			in.close();
		}
		return data;
	}
	
	
	public double getEuclidean2(double[] v1, double[] v2) {
		double sum = 0;
		for (int i = 0; i < vectorSize; i++) {
			sum += ((v1[i] - v2[i]) * (v1[i] - v2[i]));
		}
		return sum;
	}
	
	public double getSparkDistance(double[] v1, double[] v2) {
		double sum = 0;
		Vector vec1 = new Vector(v1);
		Vector vec2 = new Vector(v2);
		return vec1.squaredDist(vec2);		
	}

	public static void usage() {
		System.out.println("Usage: java edu.indiana.salsa.kmeans.SeqDistanceCalcTester <data input path> <centroids path> " 
				+ "<number of data points> <vector size>");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length < 4) {
			usage();
			System.exit(-1);
		}
		try {
			SeqDistanceCalcTester tester = new SeqDistanceCalcTester(args[0], args[1], Integer.valueOf(args[2]), Integer.valueOf(args[3]));
			tester.testDistanceCalcJava();
			tester.testDistanceCalcSpark();
		} catch (Exception e) {
			e.printStackTrace();
			usage();
			System.exit(-1);
		}
	}

}
