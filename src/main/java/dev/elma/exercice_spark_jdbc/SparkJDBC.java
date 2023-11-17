package dev.elma.exercice_spark_jdbc;

import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class SparkJDBC {
    public static void main(String[] args) {
        HashMap<String,String> db=new HashMap<>();

                .option()
                .option("url", "jdbc:mysql://localhost:3306/db_hopital_spark")
                .option("user","root")
                .option("password","")
        SparkSession spark = SparkSession.builder().appName("app3").master("local[*]").getOrCreate();

        spark.read().format("jdbc")
                .options().

    }
}
