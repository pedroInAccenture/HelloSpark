package sql

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.LoadConf

object ConvertParquetToParquet extends App{

  /**
   * Spark settings
   *
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

  //leer archivos parquet
  val data = spark.read
    .format("parquet").load(conf.getString("output.pathTres")) //toma archivo parquet

  /**
   * TRANSFORMATIONS
   */

  val dataTransformed = data.select(col("*"), lit("12").as("otro"))


  /**
   * INPUTS
   */

  logger.info("=====> Writing file") //log
  //convertir avro a parquet
  dataTransformed.write.format("parquet").mode(SaveMode.Overwrite)
    .save(conf.getString("output.pathCuatro")) //aqui se descarga el parquet

  logger.info("=====> sleeping")


}
