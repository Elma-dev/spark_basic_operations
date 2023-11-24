package dev.elma.exercice_spark_jdbc;

import org.apache.spark.sql.*;

import java.util.Properties;

import static org.apache.spark.sql.functions.col;

public class SparkJDBC {
    public static void main(String[] args) throws AnalysisException {
        //create session
        SparkSession spark = SparkSession.builder().appName("app3").master("local[*]").getOrCreate();
        //dataBase Properties
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","");
        String url="jdbc:mysql://localhost:3306/sparkDb";
        //Get DataTables
        Dataset<Row> consultationsData = spark.read().jdbc(url,"consultations",connectionProperties);
        Dataset<Row> medicines = spark.read().jdbc(url, "medicins", connectionProperties);
        Dataset<Row> patients = spark.read().jdbc(url, "patients", connectionProperties);




        //Show schema
        consultationsData.printSchema();
        medicines.printSchema();
        patients.printSchema();

        //Number Of consultations in day
        Dataset<Row> dateConsultation = consultationsData.groupBy(col("date_consultation")).count();
        System.out.println("number of consultations");
        dateConsultation.show();
        //Number Of Consultations do by medicine
        medicines.createTempView("medicines");
        consultationsData.createTempView("consultations");
        Dataset<Row> medCons = spark.sql("select * from medicines as m join consultations as c on m.id=c.id_medecin");
        System.out.println("number of consultations do by each medicines");
        medCons.groupBy(col("nom"),col("prenom")).count().withColumnRenamed("count","Number_Consultations").show();

        //Number Of Patient consulted by medicines
        patients.createTempView("patients");
        medCons.createTempView("medCons");
        Dataset<Row> mcp = spark.sql("select mc.nom,mc.prenom,p.cin from medCons as mc join patients as p where p.id=mc.id_patient");
        System.out.println("number of patients consulted do by each medicines ");
        mcp.groupBy(col("nom"),col("prenom")).count().show();
        spark.stop();


    }
}
