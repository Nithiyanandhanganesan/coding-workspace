package twitterStream;


import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

/**
 * <p>This is a code example of Twitter4J Streaming API - sample method support.<br>
 * Usage: java twitter4j.examples.PrintSampleStream<br>
 * </p>
 *
 * @author Yusuke Yamamoto - yusuke at mac.com
 */
public final class PrintSampleStream {
    /**
     * Main entry of this application.
     *
     * @param args arguments doesn't take effect with this example
     * @throws TwitterException when Twitter service or network is unavailable
     */
    public static void main(String[] args) throws TwitterException {
    	
    	ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("nqK2EXHbFRb17qX0qi6d8JzTd")
		  .setOAuthConsumerSecret("YlC7YkTW5gG1pmT4A2RuYiH9wW1XyrZtDAF1Tcn8BPsg18oSAY")
		  .setOAuthAccessToken("765955622688944128-ZS2XL75oUmZst93u9AmOHzMZqukErRf")
		  .setOAuthAccessTokenSecret("tAy57ePIvWaPNyS5psl1nt1tFpZWaKUg12nGwHbNPpm1r");
		
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
            
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

           
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

         
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        
        long[] followArray =new long[10];

        
        twitterStream.addListener(listener);
        twitterStream.user();
        twitterStream.
 //       twitterStream.sample();
        
    }
}
