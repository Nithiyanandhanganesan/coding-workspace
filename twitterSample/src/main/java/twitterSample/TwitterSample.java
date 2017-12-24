package twitterSample;

//import TwitterKafkaProducer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;







import twitter4j.DirectMessage;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class TwitterSample {

	private static final String topic = "twitter-topic";

	public static void main(String[] args) throws TwitterException
	{

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("nqK2EXHbFRb17qX0qi6d8JzTd")
		  .setOAuthConsumerSecret("YlC7YkTW5gG1pmT4A2RuYiH9wW1XyrZtDAF1Tcn8BPsg18oSAY")
		  .setOAuthAccessToken("765955622688944128-ZS2XL75oUmZst93u9AmOHzMZqukErRf")
		  .setOAuthAccessTokenSecret("tAy57ePIvWaPNyS5psl1nt1tFpZWaKUg12nGwHbNPpm1r");
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		
		//post tweet
		// The factory instance is re-useable and thread safe.
//	    Status status = twitter.updateStatus("watched @kabali movie & its good");
//	    System.out.println("Successfully updated the status to [" + status.getText() + "].");
	    
	    //read timeline
	    // The factory instance is re-useable and thread safe.
		//First param of Paging() is the page number, second is the number per page 
	    Paging paging = new Paging(1, 100);
	    //Below command to read from our home time line
//	    List<Status> status = twitter.getHomeTimeline();
	    //Below command to read from others home time line
	    List<Status> statuses = twitter.getUserTimeline("anilkumble1074",paging);
	    System.out.println("Showing home timeline.");
	    for (Status status_read : statuses) {
	        System.out.println(status_read.getUser().getName() + ":" +
	                           status_read.getText());
	     
	/*     // The factory instance is re-useable and thread safe.
	     Query query = new Query("kabali");
	     QueryResult result = twitter.search(query);
	     for (Status status_search : result.getTweets()) {
	        System.out.println("@" + status_search.getUser().getScreenName() + ":" + status_search.getText());
	        }	  */   
	    
	    }
	    
	   
	}

}
