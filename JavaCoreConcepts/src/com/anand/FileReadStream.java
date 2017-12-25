package com.anand;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

public class FileReadStream{
	
	public void readBytes()
	{
		try 
		{
			InputStream input = new FileInputStream("/Users/nwe.nganesan/Desktop/test1");
			int data;
			while ((data=input.read()) !=-1)
			{
				byte byteVal = (byte) data;
				System.out.println(byteVal);
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();

		} 
		
	}
	
	public void readArrayBytes()
	{
		try 
		{
			InputStream input = new FileInputStream("/Users/nwe.nganesan/Desktop/test1");
			int length;
			byte[] byteBuff = new byte[10];
			while ((length = input.read(byteBuff))>=10)
			{
				for(int i=0;i<length;i++)
				   {
				      byte byteVal = byteBuff[i];
				      System.out.println(byteVal);
				   }
				System.out.println("one batch");
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readChar()
	{
		try 
		{
			Reader input = new FileReader("/Users/nwe.nganesan/Desktop/test1");
			int data;
			while ((data = input.read())!=-1)
			{
				char inputChar = (char) data;
				System.out.println(inputChar);
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void readCharArray()
	{
		try 
		{
			Reader input = new FileReader("/Users/nwe.nganesan/Desktop/test1");
			int length;
			char[] inputChar = new char[10];
			while ((length = input.read(inputChar))>=10)
			{
				for(int i=0;i<length;i++)
				   {
				      char charVal = inputChar[i];
				      System.out.println(charVal);
				   }
				System.out.println("one batch");
			}
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		FileReadStream file = new FileReadStream();
		//file.readBytes();
		//file.readArrayBytes();
		//file.readChar();
		file.readCharArray();
		
    }

}
