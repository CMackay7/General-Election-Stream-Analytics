package com.kaviddiss.storm;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.api.SearchResource;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;
	private TwitterStream twitterStream;

	//The function which declares and starts the twitter stream
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;
		
		// The only coded needed for the statusListener is the the code in the onStatus 
		StatusListener tweetlistener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				//As soon as a status is passed to the code send it to the next bolt in the topology
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			@Override
			public void onStallWarning(StallWarning stallWarning) {
			}

			@Override
			public void onException(Exception e) {
			}
		};
		
		//Declarations for the running of the twitterStream
		FilterQuery hashtagquery = new FilterQuery();
		TwitterStreamFactory factory = new TwitterStreamFactory();
		twitterStream = factory.getInstance();
		
		// Filter for the given hashtags
		String[] track = {"#GE2019"};
		hashtagquery.track(track);
		
		
		twitterStream.addListener(tweetlistener);
		// Start and filter the twitterStream
		twitterStream.sample();
		twitterStream.filter(hashtagquery);
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		// if there are no tweets in the queue wait for some time until you poll the queue again
		if (ret == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
