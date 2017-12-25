package com.anand;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class AlterTable {
	 public static void main(String[] args) throws IOException {

	      // Instantiating configuration class
	      Configuration con = HBaseConfiguration.create();

	      // Instantiating HbaseAdmin class
	      HBaseAdmin admin = new HBaseAdmin(con);

	      // Instantiating columnDescriptor class
	      HColumnDescriptor columnDescriptor = new HColumnDescriptor("contactDetails");
	      
	      // Adding column family
	      admin.addColumn("emp", columnDescriptor);
	      System.out.println("coloumn added");
	      
	      // Deleting a column family
	      admin.deleteColumn("emp","contactDetails");
	      System.out.println("coloumn deleted"); 
	      
	     // disabling table named emp
	      admin.disableTable("emp12");

	      // Deleting emp
	      admin.deleteTable("emp12");
	      System.out.println("Table deleted");
	   }
}
