package com.example;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class App {
    public static void main(String[] args) throws InterruptedException {
        // set logger so no messy messages
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // Create a local StreamingContext with two working thread and batch interval of
        // 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // use spark streaming
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        // print every word
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairDStream<String, Integer> pair_count = pairs.reduceByKey((curr, accum) -> curr + accum);
        pair_count.print(); // print every result out

        // start streaming data
        jssc.start();
        jssc.awaitTermination(); // wait for termination

    }   
}
