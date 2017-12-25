package com.wrs.datamanagement.util;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.hbase.client.Connection;

public class BigTableConnector {

  private static BigTableConnector bigTableConnector = null;
  private Connection btConnection = null;

  private BigTableConnector(String projectId, String bigTableInstanceId) {
    btConnection = BigtableConfiguration.connect(projectId, bigTableInstanceId);
  }

  public static  BigTableConnector getInstance(String projectId, String bigTableInstanceId){
    if(bigTableConnector == null)
    {
      bigTableConnector = new BigTableConnector(projectId, bigTableInstanceId);
    }
    return bigTableConnector;
  }

  public Connection getBigTableConnection() {
    return btConnection;
  }

}

