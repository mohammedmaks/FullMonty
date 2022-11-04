package fullmonty;

/*******************************************************************************
 * Copyright (C) 2017, 2019 Push Technology Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

import com.pushtechnology.diffusion.client.Diffusion;
import com.pushtechnology.diffusion.client.features.TopicUpdate;
import com.pushtechnology.diffusion.client.features.Topics;
import com.pushtechnology.diffusion.client.features.Topics.ValueStream;
import com.pushtechnology.diffusion.client.features.control.clients.SystemAuthenticationControl;
import com.pushtechnology.diffusion.client.features.control.topics.SessionTrees;
import com.pushtechnology.diffusion.client.features.control.topics.SubscriptionControl;
import com.pushtechnology.diffusion.client.features.control.topics.TopicControl;
import com.pushtechnology.diffusion.client.features.control.topics.views.TopicViews;
import com.pushtechnology.diffusion.client.session.Session;
import com.pushtechnology.diffusion.client.topics.details.TopicSpecification;
import com.pushtechnology.diffusion.client.topics.details.TopicType;
import com.pushtechnology.diffusion.examples.ClientSimpleSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.pushtechnology.diffusion.client.Diffusion.newBranchMappingTableBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.sql.Time;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

/**
 * Full monty task
 *
 * @author Liman Makama
 * 
 */
public final class FullMonty {

	private static final Logger LOG = LoggerFactory.getLogger(ClientSimpleSubscriber.class);

	static String randomString() {

		// chose a Character random from this String
		String randomString = "abcdefghijklmnopqrstuvxyz";

		// create StringBuffer size of string
		StringBuilder sb = new StringBuilder(10);

		for (int i = 0; i < 10; i++) {

			// generate a random number between
			// 0 to string variable length
			int index = (int) (randomString.length() * Math.random());

			// add Character one by one in end of sb
			sb.append(randomString.charAt(index));
		}

		return sb.toString();
	}

	/**
	 * Main.
	 */
	public static void main(String... arguments) throws InterruptedException, ExecutionException, TimeoutException {

		class FooStream extends ValueStream.Default<String> {
			@Override
			public void onValue(String topicPath, TopicSpecification specification, String oldValue, String newValue) {
				LOG.info(newValue);
			}

			@Override
			public void onSubscription(String s, TopicSpecification topicSpecification) {
				System.out.println("Subscribe to" + s);
				LOG.info(s);
			}
		}

		Set<String> userProp = new HashSet<String>();

		userProp.add("gold");
		userProp.add("silver");
		userProp.add("platinum");

		// Connect using a principal with 'modify_topic' and 'update_topic'
		// permissions

		// high volume connector port 8090
		Session session = Diffusion.sessions().principal("admin").password("password").open("ws://localhost:8090");

		// Allows the server to accept proposed session property tier with the values
		// silver, gold and platinum.
		final SystemAuthenticationControl authControl = session.feature(SystemAuthenticationControl.class);
		final SystemAuthenticationControl.ScriptBuilder builder = authControl.scriptBuilder();

		builder.trustClientProposedPropertyIn("tier", userProp);
		String script = builder.script();
		authControl.updateStore(script).get(10, SECONDS);

		// Get the TopicControl and TopicUpdate feature
		final TopicControl topicControl = session.feature(TopicControl.class);
		final Topics topics = session.feature(Topics.class);
		final TopicUpdate topicUpdate =  session.feature(TopicUpdate.class);

		topics.addStream("?.*//", String.class, new FooStream());
		/*
		 * topics.addStream("data/sink", String.class, new Topics.ValueStream<String>()
		 * {
		 * 
		 * @Override public void onValue(String s, TopicSpecification
		 * topicSpecification, String s2, String v1) { System.out.println(v1 +
		 * "new value");
		 * 
		 * }
		 * 
		 * @Override public void onSubscription(String s, TopicSpecification
		 * topicSpecification) { System.out.println("Subscribe to" + s); LOG.info(s); }
		 * 
		 * @Override public void onUnsubscription(String s, TopicSpecification
		 * topicSpecification, Topics.UnsubscribeReason unsubscribeReason) {
		 * 
		 * }
		 * 
		 * @Override public void onClose() {
		 * 
		 * }
		 * 
		 * @Override public void onError(ErrorReason errorReason) {
		 * System.out.println("Error" + errorReason); } });
		 */

		// Create an int64 topic 'foo/counter'
		// final CompletableFuture<TopicControl.AddTopicResult> future =
		// topicControl.addTopic(
		// "foo/counter",
		// TopicType.INT64);
		// full monty code testing
		// Creates 100 topics using the topic path data/sink/<#>, where <#> is a number
		// from 1 to 100
		for (long i = 1; i <= 100; i++) {
			topicControl.addTopic("data/sink/" + i, TopicType.STRING).get(10, TimeUnit.SECONDS);
			// topicControl.addTopic("data/sink/" +i, spec);
		}

		// Create a topic view of each topic path at data/sink/<#> to views/silver/<#>
		// throttled to an update every 10 seconds
		// delayed by 5 seconds
		session.feature(TopicViews.class).createTopicView("view1",
				"map ?data/sink// to views/silver/<path(2)> throttle to 1 updates every 10 seconds delay by 5 seconds");

		// Create a topic view of each topic path at data/sink/<#> to views/gold/<#>
		// without any other wrangling
		session.feature(TopicViews.class).createTopicView("view2", "map ?data/sink// to views/gold/<path(2)>");
		// Wait for the CompletableFuture to complete
		// future.get(10, TimeUnit.SECONDS);

		session.feature(TopicViews.class).createTopicView("view3",
				"map ?data/sink// to views/platinum/<path(2)> type TIME_SERIES");

		// Create a session trees mapping table for the topic path welcome/ with the
		// following routing logic:
		// if the tier is silver, then go to views/silver
		// if the tier is gold, then go to views/gold
		// if the tier is platinum then go to views/platinum
		final SessionTrees.BranchMappingTable branchMappingTable = newBranchMappingTableBuilder()
				.addBranchMapping("tier is 'silver'", "views/silver").addBranchMapping("tier is 'gold'", "views/gold")
				.addBranchMapping("tier is 'platinum'", "views/platinum").create("welcome/");

		session.feature(SessionTrees.class).putBranchMappingTable(branchMappingTable).get(5, SECONDS);

		// In a loop, every second, all 100 topics are to be updated with 5 times, with
		// random strings of length 10.
		// this loop will continue until 60 seconds have elapsed.
		
		LocalTime before_timestamp = LocalTime.now();
		for (int i = 0; i < 5; i++) {
			for(int j = 0; j < 100; j++) {
				String random_string = randomString();
				topicUpdate.set(String.format("data/sink/%s",j),String.class, random_string);
			}
		}		
		LocalTime finished_timestamp = LocalTime.now();
		
//		long elapsed_time = before_timestamp.until(finished_timestamp, (TemporalUnit) ChronoUnit.SECONDS) ;
		
		float elapsed_time = (ChronoUnit.MILLIS.between(before_timestamp, finished_timestamp));
		elapsed_time = elapsed_time/1000;
		
		System.out.println("now listening " +elapsed_time);
		//topics.subscribe("?data/sink/");
		Thread.sleep(10000);

		System.out.println("done");

	}
}
