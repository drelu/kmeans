package com.drelu;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
//import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class SimpleKMeansClustering {
	public static final double[][] points = { {1.1, 1}, {2, 1}, {1, 2},
		{2, 2}, {3, 3}, {8, 8},
		{9, 8}, {8, 9}, {9, 9}};

	public static void writePointsToFile(List<Vector> points,
			String fileName,
			FileSystem fs,
			Configuration conf) throws IOException {
		Path path = new Path(fileName);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, DoubleWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec = new VectorWritable();
		for (Vector point : points) {
			vec.set(point);
			writer.append(new DoubleWritable(recNum++), vec);
		}
		writer.close();
	}

	public static List<Vector> getPoints(String filename) {
		System.out.println("Read points from: " + filename);
		List<Vector> points = new ArrayList<Vector>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			while (line != null) {
				String components[] = line.split(",");
				//double fr[] = new double[components.length];
				double fr[] = new double[2];
				for (int i=0; i<2; i++){
					fr[i]=Double.parseDouble(components[i]);
				}
				Vector vec = new RandomAccessSparseVector(fr.length);
				vec.assign(fr);
				points.add(vec);		
				line = br.readLine();
			}
		
		} catch (Exception e){
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return points;
	}


	public static void main(String args[]) throws Exception {
		int k = 2;

		if (args.length<1){
			System.out.println("Usage: java -jar kmeans-1.0-SNAPSHOT.jar <path-to-input-file>");

		}
		List<Vector> vectors = getPoints(args[0]);

		//		File testData = new File("testdata");
		//		if (!testData.exists()) {
		//			testData.mkdir();
		//		}
		//		
		//		testData = new File("testdata/points");
		//		if (!testData.exists()) {
		//			testData.mkdir();
		//		}

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		writePointsToFile(vectors, "kmeans/points/file1", fs, conf);

		Path path = new Path("kmeans/clusters/part-00000");
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
				path, Text.class, Kluster.class);

		for (int i = 0; i < k; i++) {
			Vector vec = vectors.get(i);
			Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
			writer.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer.close();

		KMeansDriver.run(conf, new Path("kmeans/points"), new Path("kmeans/clusters"),
				new Path("output"), new EuclideanDistanceMeasure(), 0.001, 10,
				true, 0.0, false);
		//		
		//KMeansDriver.runJob("testdata", "output/clusters-0", "output",
		//		EuclideanDistanceMeasure.class.getName(), "0.001", "10", true);


		SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				new Path("output/" + Kluster.CLUSTERED_POINTS_DIR
						+ "/part-m-00000"), conf);

		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		while (reader.next(key, value)) {
			System.out.println(value.toString() + " belongs to cluster "
					+ key.toString());
		}
		reader.close();
	}

}
