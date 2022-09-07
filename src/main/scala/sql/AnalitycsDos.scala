package sql


import com.typesafe.config.Config
import org.apache.hadoop.shaded.org.jline.keymap.KeyMap.display
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object AnalitycsDos extends App {

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
    .csv(conf.getString("input.pathDos")) //Lee el archivo del conf el de clientes csv


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = df.select(col("*"),lit(3).as("literal"))


  /**
   * INPUTS
   */
  logger.info("=====> Writing file")


  dfTransformed.write.format("avro").mode(SaveMode.Overwrite)
    .save(conf.getString("output.pathDos"))


  logger.info("=====> sleeping")
  //  Thread.sleep(1000000)

//  logger.info("=====> Reading file avro")
//
//  val data = spark.read.format("avro").load(conf.getString("output.pathDos"))
//
//  println(data)


}
