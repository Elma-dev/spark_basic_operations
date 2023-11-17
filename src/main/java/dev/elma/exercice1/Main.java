package dev.elma.exercice1;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static void main(String[] args) throws AnalysisException {
        // create session
        SparkSession spark = SparkSession.builder().appName("AppSql").master("local[*]").getOrCreate();
        //read json data
        Dataset<Row> data = spark.read().option("multiline", true).json("data.json");
        //do show action to get data

            //data.show(5);
        Dataset<Row> product = data.select("product");
        data.createTempView("produits");
        spark.sql("select * from produits").show();
        spark.stop();


    }
}