package com.github.jaskey.consistenthash.sample;

import com.github.jaskey.consistenthash.ConsistentHashRouter;
import com.github.jaskey.consistenthash.Node;
import de.hasenburg.broker.simulation.main.MainKt;
import de.hasenburg.geobroker.commons.model.message.Topic;

import java.util.ArrayList;
import java.util.List;

/**
 * Topic Node Example
 *
 * @author jonathanhasenburg
 */
class TopicNode implements Node {

	private Topic topic;

	public TopicNode(Topic topic) {
		this.topic = topic;
	}

	@Override
	public String getKey() {
		return topic.getTopic();
	}

	@Override
	public String toString() {
		return "TopicNode{ " + topic.getTopic() + " }";
	}

	public static void main(String[] args) {

		List<TopicNode> topicNodeList = new ArrayList<>();
		for (String st : MainKt.generateRandomStrings("tN-", 1, 3, 12345)) {
			topicNodeList.add(new TopicNode(new Topic(st)));
		}

		List<String> testTopics = new ArrayList<>(MainKt.generateRandomStrings("t-", 3, 10, 12345));

		System.out.println("Initial Setup");
		ConsistentHashRouter<TopicNode> hashRouter = new ConsistentHashRouter<>(topicNodeList, 10);
		printTopicRoutes(hashRouter, testTopics);

		System.out.println("Adding tN-Amazing");
		hashRouter.addNode(new TopicNode(new Topic("tN-Amazing")), 10);
		printTopicRoutes(hashRouter, testTopics);

		System.out.println("Remove topic node " + topicNodeList.get(0));
		hashRouter.removeNode(topicNodeList.get(0));
		printTopicRoutes(hashRouter, testTopics);

		System.out.println("Remove topic node " + topicNodeList.get(1));
		hashRouter.removeNode(topicNodeList.get(1));
		printTopicRoutes(hashRouter, testTopics);
	}

	private static void printTopicRoutes(ConsistentHashRouter<TopicNode> hashRouter, List<String> topics) {
		for (String topic : topics) {
			System.out.println(topic + " is routed to " + hashRouter.routeNode(topic));
		}
	}

}
