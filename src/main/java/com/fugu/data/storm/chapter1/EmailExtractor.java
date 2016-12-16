package com.fugu.data.storm.chapter1;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EmailExtractor extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("email"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String commit = input.getStringByField("commit");
        String[] parts = commit.split(" ");
        collector.emit(new Values(parts[1]));
    }
}
