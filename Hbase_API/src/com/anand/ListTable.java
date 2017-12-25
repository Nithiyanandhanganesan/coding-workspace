package com.anand;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class ListTable {
	 public static void main(String[] args) throws IOException {

	      // Instantiating configuration class
	      Configuration con = HBaseConfiguration.create();

	      // Instantiating HbaseAdmin class
	      HBaseAdmin admin = new HBaseAdmin(con);

	   // Getting all the list of tables using HBaseAdmin object
	      HTableDescriptor[] tableDescriptor = admin.listTables();
	      
	      // Verifying the existance of the table
	      boolean bool = admin.tableExists("emp");
	      System.out.println( bool);

	      // printing all the table names.
	      for (int i=0; i<tableDescriptor.length;i++ ){
	         System.out.println(tableDescriptor[i].getNameAsString());
	      }
	      
	      // Verifying weather the table is disabled
	      Boolean bool2 = admin.isTableDisabled("emp");
	      System.out.println(bool2);

	      // Disabling the table using HBaseAdmin object
	      if(!bool2){
	         admin.disableTable("emp");
	         System.out.println("Table disabled");
	      }
	      
	   // Verifying weather the table is disabled
	      Boolean bool1 = admin.isTableEnabled("emp");
	      System.out.println(bool1);

	      // Disabling the table using HBaseAdmin object
	      if(!bool1){
	         admin.enableTable("emp");
	         System.out.println("Table Enabled");
	      }
	   }
}
