package dev.elma.exercice2;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

public class Exercice2 {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().appName("app2").master("local[*]").getOrCreate();
        Dataset<Row> data = spark.read().option("header", true).option("inferSchema",true).csv("dataDev.csv");
        //Q1
            //data.groupBy("service").count().show();
        //Q2
        data.groupBy(year(col("date")), col("service") ).count().orderBy(col("count").desc()).show();

    }
}
