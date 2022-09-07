package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object AnalyticsDos extends App {

  /**
   *  Spark settings
   */
  val logger = Logger.getLogger(this.getClass.getName)

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()


  /**
   *  PARAMETERS
   */
  val conf:Config = LoadConf.getConfig


  /**
   * INPUTS
   */
  logger.info("=====> Reading file Avro")

  val df = spark.read.format(source = "avro").load(conf.getString("output.pathDos"))


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = df.select(col("*"),lit(3)).as(alias = "pathDos")


  /**
   * INPUTS
   */
  logger.info("=====> Writing file")
  dfTransformed.write.format("avro").mode(SaveMode.Overwrite)
    .save(conf.getString("output.pathDos"))

//    .csv(conf.getString("output.path"))


  logger.info("=====> sleeping")
  //  Thread.sleep(1000000)

}

