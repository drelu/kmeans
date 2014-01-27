package com.drelu.random;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;



public class RandomData {

	public final static int dimensions = 3;
	// Scenario 1
	//public final static long numOfPointsPerFile = 100000000l * 500l;
	
	// Scenario 2
	//public final static long numOfPointsPerFile = 10000000l * 5000l;
	
	// Scenario 3
	public final static long numOfPointsPerFile = 1000000l * 50000l;
	
	// Scenario 4 (tiny)
	//public final static long numOfPointsPerFile = 1 * 5000;
	

	public static void main(String[] args) {
		Random random = new Random();
		PrintWriter writer=null;
		try {
			writer = new PrintWriter("random_" + numOfPointsPerFile + "points.csv", "UTF-8");
			for (long i = 0; i < numOfPointsPerFile; i++) {
				for (int j = 0; j < dimensions; j++) {
					double point = random.nextDouble() * 1000;
					writer.print(String.format("%.0f", point));
          if (j<dimensions-1){
					  writer.print(",");
          }
				}
				writer.print("\n");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			writer.close();
		}
	}

}
