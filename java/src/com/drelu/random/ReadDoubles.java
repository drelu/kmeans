package com.drelu.random;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

public class ReadDoubles {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataInputStream din = null;
		long counter = 0l;
		try
		{
			din = new DataInputStream(new FileInputStream("data_1mPoints5000Centroids24mappers_part0"));
			while (true)
			{
				Double data = din.readDouble();
				counter++;
				System.out.printf("\n-> %s \n ", data);				
			}
			
		}
		catch (EOFException ignore)
		{
		}
		catch (Exception ioe)
		{
			ioe.printStackTrace();
		}
		finally
		{
			if (din != null)
			{
				try
				{
					din.close();
				}
				catch (IOException e1)
				{
					e1.printStackTrace();
				}
			}
		}
		System.out.printf("Number values: " + counter);
	}
}


