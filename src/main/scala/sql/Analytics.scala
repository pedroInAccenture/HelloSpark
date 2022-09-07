package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object Analytics extends App {

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
  logger.info("=====> Reading file")

  val df = spark.read
    .option("header",true)
    .csv(conf.getString("input.path"))


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = df.select(col("*"),lit(1).alias("literal"))


  /**
   * OUTPUT
   */
  logger.info("=====> Writing file")
  dfTransformed.write.mode("overwrite")
    .csv(conf.getString("output.path"))


  logger.info("=====> sleeping")
  logger.warn("=====> sleeping")
  logger.error("=====> sleeping")
//  Thread.sleep(1000000)

}

