package com.fugu.data.storm.chapter1;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class EmailCounter extends BaseBasicBolt {
    private Map<String, Integer> counts;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String email = input.getStringByField("email");
        counts.put(email, count(email) + 1);
        printCount();
    }

    private int count(String email) {
        Integer count = counts.get(email);
        return (count == null) ? 0 : count;
    }

    private void printCount() {
        System.out.println("Starting printing count");
        counts.entrySet()
                .forEach(entry ->
                    System.out.println(
                            String.format("Email %s has %s counts", entry.getKey(), entry.getValue())
                    ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt doesn't emit anything
    }
}
