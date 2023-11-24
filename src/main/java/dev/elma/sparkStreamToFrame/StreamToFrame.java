package dev.elma.sparkStreamToFrame;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

public class StreamToFrame {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("StreamFrame").master("local[*]").getOrCreate();
        Dataset<Row> inputData = spark.readStream().format("socket").
                option("host", "localhost").
                option("port", "8888").
                load();
        Dataset<String> stringDataset = inputData.as(Encoders.STRING());
        Dataset<String> words = stringDataset.flatMap(
                (FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(),
                Encoders.STRING());
        Dataset<Row> count = words.groupBy("value").count();

        StreamingQuery query = count.writeStream().outputMode("update").format("console").start();
        query.awaitTermination();

    }
}
