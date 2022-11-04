import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.ValueStream;
import com.pushtechnology.diffusion.client.features.control.topics.SubscriptionControl.SubscriptionByFilterResult;
import com.pushtechnology.diffusion.client.features.control.topics.SubscriptionControl;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.examples.ClientSimpleSubscriber;

/**
 * 
 */

/**
 * @author lmakama
 *
 */
public class FullMontySubscriber {

	/**
	 * @param args
	 */
	
	private static final Logger LOG =
	        LoggerFactory.getLogger(ClientSimpleSubscriber.class);
	
	/*
	 * public String subscribeToTiers(String tier) {
	 * 
	 * String result = ""; if (tier.equals("silver")) { result = tier; } else if
	 * (tier.equals("gold")) { result = tier; }
	 * 
	 * return }
	 */
	
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		
		Session session = Diffusion.sessions().property("tier", "silver").principal("admin").password("password").open("ws://localhost:8080");

//		final SubscriptionControl subscriptionControl = session.feature(SubscriptionControl.class);
//		final String filter = "tier is 'silver'";

		final Topics topics = session.feature(Topics.class);
		
		/**
	     * A topic stream that prints updates to the console.
	     */
	    class ValueStreamPrintLn extends ValueStream.Default<String> {
	        @Override
	        public void onValue(
	            String topicPath,
	            TopicSpecification specification,
	            String oldValue,
	            String newValue) {
	            System.out.println(topicPath + ":   " + newValue);
	        }
	        
	        @Override 
	        public void onSubscription(String s, TopicSpecification topicSpecification) { 
	        	System.out.println("Subscribe to" + s); 
	        	LOG.info(s); }
	    }

		topics.addStream("?.*//", String.class, new ValueStreamPrintLn());		
		
	    topics.subscribe("?data/sink/");
//		subscriptionControl.subscribeByFilter(filter, "?views/silver/");	
	    
	    Thread.sleep(10000);
		
		System.out.println("done");
		
	}

}
