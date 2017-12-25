package com.anand;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

public class FileWriteStream {
	
	public void writeBytes()
	{
		try 
		{
			OutputStream output = new FileOutputStream("/Users/nwe.nganesan/Desktop/testoutput.txt");
			byte byteVal = 100;
			output.write(byteVal);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();

		} 
		
	}
	
	public void writeArrayBytes()
	{
		try 
		{
			OutputStream output = new FileOutputStream("/Users/nwe.nganesan/Desktop/testoutput.txt");
			
			byte[] byteBuff = {'a','b','c'};
			output.write(byteBuff);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeChar()
	{
		try 
		{
			Writer output = new FileWriter("/Users/nwe.nganesan/Desktop/testoutput.txt");
			int charVal = 100;
		    output.write(charVal);
		    output.flush();
		    output.close();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeCharArray()
	{
		try 
		{
			Writer output = new FileWriter("/Users/nwe.nganesan/Desktop/testoutput.txt");
			char[] charBuff= {'a','b','c'};
			output.write(charBuff);
			output.flush();
			output.close();
			
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeString()
	{
		try 
		{
			Writer output = new FileWriter("/Users/nwe.nganesan/Desktop/testoutput.txt");
			String data="hello world";
			output.write(data);
			//flush is used to flush the output to file.
			output.flush();
			output.close();
			
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		FileWriteStream file = new FileWriteStream();
		//file.writeBytes();
		//file.writeArrayBytes();
		//file.writeChar();
		//file.writeCharArray();
		file.writeString();
		
    }

}
