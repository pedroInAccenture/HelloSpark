package sql

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.LoadConf
//Convertir Avro a Parquet
object ConvertAvroToParquet extends App{
  /**
   * Spark settings
   * cargar la configuracion del spark junto con el logger
   *
   */

  val logger = Logger.getLogger(this.getClass.getName)

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()


  /**
   * PARAMETERS
   */

  val conf: Config = LoadConf.getConfig


  /**
   * INPUTS
   */

  logger.info("=====> Reading file")

  //leer archivos avro
  val data = spark.read
    .format("avro")
    .load(conf.getString("output.pathDos")) //toma archivo avro


  /**
   * TRANSFORMATIONS
   */

  val dataTransformed = data.select(col("*"),lit("1").as("literal2"))


  /**
   * INPUTS
   */

  logger.info("=====> Writing file") //log
  //convertir avro a parquet
  dataTransformed.write.format("parquet").mode(SaveMode.Overwrite)
    .save(conf.getString("output.pathTres")) //aqui se descarga el parquet

  logger.info("=====> sleeping")


}
