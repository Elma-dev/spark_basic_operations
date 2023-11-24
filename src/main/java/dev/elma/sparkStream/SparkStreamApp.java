package dev.elma.sparkStream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;

public class SparkStreamApp {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkContext = new SparkConf().setAppName("sparkContext").setMaster("local[*]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(16));
        JavaReceiverInputDStream<String> textStream = javaStreamingContext.socketTextStream("localhost", 8888);
        ReceiverInputDStream<String> stringReceiverInputDStream = textStream.receiverInputDStream();
        stringReceiverInputDStream.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
