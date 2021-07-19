package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class Topology {

	static final String TOPOLOGY_NAME = "storm-twitter-party-count";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);

		TopologyBuilder b = new TopologyBuilder();
		b.setSpout("TwitterSampleSpout", new TwitterSpout());
        b.setBolt("WordSplitterBolt", new WordSplitterBolt(2)).shuffleGrouping("TwitterSampleSpout");
        b.setBolt("PartyFilterBolt", new PartyFilterBolt()).shuffleGrouping("WordSplitterBolt");
        b.setBolt("PartyCounterBolt", new PartyCounterBolt(5)).shuffleGrouping("PartyFilterBolt");

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				cluster.killTopology(TOPOLOGY_NAME);
				cluster.shutdown();
			}
		});

	}

}
