package com.Bumblebee.KafkaStormWordCountExample;


import backtype.storm.Config;
import backtype.storm.LocalCluster;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import lombok.extern.slf4j.Slf4j;
import storm.kafka.*;

import java.util.*;

/**
 * Created by smrutiranjan.s on 8/6/16.
 */
public class KafkaStormWordCountExample {

    public class StormTest1 {

        public class splitSentence extends BaseBasicBolt{  //A Bolt to Split the sentences into words and emits the words
            public void execute(Tuple tuple, BasicOutputCollector collector){
                System.out.println(tuple);
                String line = tuple.getString(0);
                System.out.println("Splitting words");
                for(String word: line.split(" "))
                    collector.emit(new Values(word));
            }
            public void declareOutputFields(OutputFieldsDeclarer declarer){
                declarer.declare(new Fields("words"));
            }
        }

        public class countWords extends BaseBasicBolt{    //A bolt to take count the words it is getting from the splitSentence bolt
            Map<String,Integer> map = new HashMap<>();

            public void execute(Tuple tuple, BasicOutputCollector collector){
                String word = tuple.getString(0);
                Integer count = map.get(word);
                if(count == null) {
                    count = 0;
                }
                count++;
                log.info("COUNTS : "+ word +" = "+ count.toString());
                map.put(word,count);
                System.out.println("Counting Words");

                collector.emit(new Values(word,map));
            }

            public void declareOutputFields(OutputFieldsDeclarer declarer){
                declarer.declare(new Fields("words","count"));
            }

            @Override
            public void cleanup(){
                for(Map.Entry<String,Integer> entry:map.entrySet())
                {
                    System.out.println(entry.getKey() +" : "+entry.getValue());
                }
            }
        }

        public void main(String args[]) throws Exception{

            ZkHosts hosts = new ZkHosts("127.0.0.1:2181");    //ZkHost is declared at localhost:2181 where we will input out data for Kafka logging
            SpoutConfig spoutConfig = new SpoutConfig(hosts,"kafkaStormTest","/tmp",UUID.randomUUID().toString());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            spoutConfig.forceFromStart = true;
            spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
            spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;

            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout("emitter",new KafkaSpout(spoutConfig),5);
            builder.setBolt("splitter",new splitSentence(),8).shuffleGrouping("emitter");
            builder.setBolt("counter",new countWords(),10).shuffleGrouping("splitter");

            Config config = new Config();
            config.setDebug(true);
            config.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCount", config, builder.createTopology());
            System.out.println("CODE END");

//        Thread.sleep(10000);
//        System.out.println("Killing the topology");
//        cluster.shutdown();
        }

    }
}
