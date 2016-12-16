package com.fugu.data.storm.chapter1;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class CommitFeedListener extends BaseRichSpout {
    private SpoutOutputCollector outputCollector;
    private List<String> commits;

    /**
     * Indicate that the spout emit a tuple with a field named "commit"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }

    /**
     * Get called when storm prepares the spout to be run
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;

        try {
            commits = IOUtils.readLines(
                    ClassLoader.getSystemResourceAsStream("changelog.txt"),
                    Charset.defaultCharset().name()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        commits.stream()
                .map(Values::new)
                .forEach(values -> outputCollector.emit(values));

    }
}
