package dev.elma.sparkStreamApplication;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.net.URL;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class SparkStreamApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession = SparkSession.builder().appName("sparkStream").master("local[*]").getOrCreate();
        //Id, titre, description, service, date
        StructType structType = new StructType(new StructField[]{
                new StructField("Id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("titre", DataTypes.StringType, false, Metadata.empty()),
                new StructField("description", DataTypes.StringType, false, Metadata.empty()),
                new StructField("service", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date", DataTypes.DateType, false, Metadata.empty())
        });
       // Dataset<Row> lines = sparkSession.readStream().option("header", true).schema(structType).csv("hospitalData.csv");
        Dataset<Row> lines = sparkSession.readStream().option("header", true).schema(structType).csv("dataDev.csv");
        StreamingQuery query = lines.writeStream().outputMode("append").format("console").start();
        query.awaitTermination();
    }
}


