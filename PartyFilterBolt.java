package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartyFilterBolt extends BaseRichBolt {
	
	private static final long serialVersionUID = 6069146554651714100L;
	
	// these variables store all the words that should be allowed to the next bolt
	// stores multiple for each party just in case users use slang 
	
	private Set<String> labourParty = new HashSet<String>(Arrays.asList(new String[] {
			"labour", "Labour"}));
	
	private Set<String> toryParty = new HashSet<String>(Arrays.asList(new String[] {
            "tories", "tory", "conservative", "conservatives"}));
	
	private Set<String> libdemParty = new HashSet<String>(Arrays.asList(new String[] {
            "libdem", "lib", "dem", "liberal", "democrats"}));
	
	private Set<String> brexitParty = new HashSet<String>(Arrays.asList(new String[] {
            "brexitparty", "brexit"}));
	
	private Set<String> greenParty = new HashSet<String>(Arrays.asList(new String[] {
            "green"}));
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    
    
    @Override
    public void execute(Tuple input) {
        String lang = (String) input.getValueByField("lang");
        String word = (String) input.getValueByField("word");
        //Decide which word should be passed to the next bolt
        
        if (labourParty.contains(word)) {
            collector.emit(new Values(lang, "labour"));
        }
        
        if (toryParty.contains(word)) {
            collector.emit(new Values(lang, "conservatives"));
        }
        
        if (libdemParty.contains(word)) {
            collector.emit(new Values(lang, "libdem"));
        }
        
        if (greenParty.contains(word)) {
            collector.emit(new Values(lang, "green"));
        }
        if (brexitParty.contains(word)) {
            collector.emit(new Values(lang, "brexit"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
