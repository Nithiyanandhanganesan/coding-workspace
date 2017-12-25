package com.anand;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class InsertRecord {
  public static void main(String args[]) throws IOException
  {
	  // Instantiating Configuration class
      Configuration config = HBaseConfiguration.create();

      // Instantiating HTable class
      HTable hTable = new HTable(config,"emp");

      // Instantiating Put class
      // accepts a row name.
      Put p = new Put(Bytes.toBytes("2")); 

      // adding values using add() method
      // accepts column family name, qualifier/row name ,value
      p.add(Bytes.toBytes("personal"),Bytes.toBytes("name"),Bytes.toBytes("kumar"));

      p.add(Bytes.toBytes("personal"),Bytes.toBytes("city"),Bytes.toBytes("pondy"));

      p.add(Bytes.toBytes("professional"),Bytes.toBytes("designation"),Bytes.toBytes("tester"));

      p.add(Bytes.toBytes("professional"),Bytes.toBytes("salary"),Bytes.toBytes("50000"));
      
      // Saving the put Instance to the HTable.
      hTable.put(p);
      System.out.println("data inserted");
      
      // closing HTable
      hTable.close();
   }
  }

