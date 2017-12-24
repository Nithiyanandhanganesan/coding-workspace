import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

	private static final String topic = "twitter-topic";

	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {

//-		Properties properties = new Properties();
//*		properties.put("metadata.broker.list", "localhost:9092");
//-		properties.put("serializer.class", "kafka.serializer.StringEncoder");
//-		properties.put("client.id","camus");
//--		ProducerConfig producerConfig = new ProducerConfig(properties);
//--		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
//--				producerConfig);
		
		System.out.println(consumerKey + consumerSecret +token + secret );
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("twitterapi",
				"#Kabali"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();
		System.out.println(queue.size());
//System.out.println("msg" + queue.take());
		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
			//	message = new KeyedMessage<String, String>(topic, queue.take());
				String msg = queue.take();
			      System.out.println(msg);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	//		producer.send(message);
		}
//--		producer.close();
/*		for (int msgRead = 0; msgRead < 1000; msgRead++) {
	          if (client.isDone()) {
	            System.out.println("Client connection closed unexpectedly: " + ((BasicClient) client).getExitEvent().getMessage());
	            break;
	          }
	          
	          String msg = queue.poll(5, TimeUnit.SECONDS);
	          if (msg == null) {
	            System.out.println("Did not receive a message in 5 seconds");
	          } else {
	            System.out.println(msg);
	          }
	        }*/
		client.stop();

	}

	public static void main(String[] args) {
		try {
		//	TwitterKafkaProducer.run(args[0], args[1], args[2], args[3]);
			TwitterKafkaProducer.run("nqK2EXHbFRb17qX0qi6d8JzTd", "YlC7YkTW5gG1pmT4A2RuYiH9wW1XyrZtDAF1Tcn8BPsg18oSAY", "765955622688944128-ZS2XL75oUmZst93u9AmOHzMZqukErRf","tAy57ePIvWaPNyS5psl1nt1tFpZWaKUg12nGwHbNPpm1r");
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}