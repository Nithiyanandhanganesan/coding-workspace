package com.anand.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	// Imports the Google Cloud client library
    	    // Instantiates a client
    	    Storage storage = StorageOptions.getDefaultInstance().getService();

    	    // The name for the new bucket
    	    String bucketName = "testbucket";  // "my-new-bucket";

    	    // Creates the new bucket
    	    Bucket bucket = storage.create(BucketInfo.of(bucketName));

    	    System.out.printf("Bucket %s created.%n", bucket.getName());
    }
}
