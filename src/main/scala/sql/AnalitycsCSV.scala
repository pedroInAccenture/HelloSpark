package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object AnalitycsCSV extends App {

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
    .csv(conf.getString("input.path")) //Lee el archivo del conf el de clientes csv


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = df.select(col("*"),lit(2).as("partitioner"))


  /**
   * INPUTS
   */
  logger.info("=====> Writing file avro partitioner")

  dfTransformed.write.partitionBy("tr")
    .format("avro").save(conf.getString("output.pathPartitionerAVRO"))

  dfTransformed.write.partitionBy("tr")
    .format("parquet").save(conf.getString("output.pathPartitionerPARQUET"))


  logger.info("=====> Termino")
  //  Thread.sleep(1000000)

}
