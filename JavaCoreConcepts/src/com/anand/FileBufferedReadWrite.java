package com.anand;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public class FileBufferedReadWrite {
	
	
	public void readChar()
	{
		try 
		{
			BufferedReader input = new BufferedReader(new FileReader("/Users/nwe.nganesan/Desktop/test1"));
			String data;
			while ((data = input.readLine())!=null)
			{
				System.out.println(data);
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readCharArray() throws IOException
	{
		BufferedReader input = null;
		BufferedWriter output = null;
		try 
		{
		    input = new BufferedReader(new FileReader("/Users/nwe.nganesan/Desktop/test5"));
			output = new BufferedWriter(new FileWriter("/Users/nwe.nganesan/Desktop/test3"));
			String data;
			String[] data1=new String[100];
			int i=0;
			while ((data = input.readLine())!=null)
			{
				data1[i]=data;
				System.out.println("Reading" +data);
				i++;
			}
			for (String d : data1)
			{
				output.write(d);
				System.out.println(d);
				output.flush();
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String args[]) throws IOException {
		FileBufferedReadWrite file = new FileBufferedReadWrite();

		//file.readChar();
		file.readCharArray();
	}
}
