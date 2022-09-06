package sql

import org.apache.spark.sql.SparkSession

object Analytics extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()


  val df = spark.read.csv("src/test/resources/data/clientes.csv")
  df.printSchema()
}
