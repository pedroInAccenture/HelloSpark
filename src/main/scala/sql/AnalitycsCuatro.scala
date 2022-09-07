package sql


import com.typesafe.config.Config
import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object AnalitycsCuatro extends App {

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
  logger.info("=====> Reading file avro")

  val data = spark.read.format("avro").load(conf.getString("output.pathTres"))


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = data.select(col("*"),lit(3).as("nuevo"))


  /**
   * INPUTS
   */
  logger.info("=====> Writing file parquet")

  dfTransformed.write.format("parquet").mode(SaveMode.Overwrite)
    .save(conf.getString("output.pathCuatro"))


  logger.info("=====> sleeping")
  //  Thread.sleep(1000000)



}
