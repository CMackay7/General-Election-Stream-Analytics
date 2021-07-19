package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class PartyCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2706047697068872387L;
	
	private static final Logger logger = LoggerFactory.getLogger(PartyCounterBolt.class);
    
    
    //Map to store the party and the number of times it has been mentioned
    private Map<String, Long> partystore;
    //Stored the time that the parties and counts were last stored this is so it doesn't display
    //every time a tweet is logged
    private final long displayTime;
    private long lastDisplay;


    public PartyCounterBolt(long intervalIn) {
        this.displayTime = intervalIn;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    	partystore = new HashMap<String, Long>();
    	lastDisplay = System.currentTimeMillis();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        
        // if the word is not already stored then add it with a count of 1
        Long count = partystore.get(word);
        if(count == null) {
        	count = 0L;
        }
        
        //increment the count for the word
        count++;
        partystore.put(word, count);
        
        // Check if the results have been displayed in the last 5 
        //seconds if not display the results
        if (timeToDisplay()) {
        	logger.info("\n\n");
        	logger.info("Word count: "+partystore.size());

            publishTopList();
            lastDisplay = System.currentTimeMillis();
        }
    }
    
    // returns true if it has been 5 seconds since the data was displayed
    public boolean timeToDisplay() {
        long now = System.currentTimeMillis();
        long timeSinceDisplay = (now - lastDisplay) / 1000;
        
        if(timeSinceDisplay > displayTime) {
        	return true;
        } else {
        	return false;
        }
    }

    //Display the results in the log
    private void publishTopList() {
        for (Map.Entry<String, Long> entry : partystore.entrySet()) {
            logger.info(new StringBuilder("top - ").append(entry.getKey()).append('|').append(entry.getValue()).toString());
        }
    }
}
