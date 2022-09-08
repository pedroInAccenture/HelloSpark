package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object analytics {

  def main(args: Array[String]): Unit = {
//    executeCsv
//    executeAvroToParquet
      executeParquetToParquet
  }

}

object executeCsv{
  /**
   * Spark settings
   */
  val logger = Logger.getLogger (this.getClass.getName)

  val spark: SparkSession = SparkSession.builder ()
  .master ("local[1]")
  .appName ("example")
  .getOrCreate ()


  /**
   * PARAMETERS
   */
  val conf: Config = LoadConf.getConfig


  /**
   * INPUTS
   */
  logger.info ("=====> Reading file")

  val df = spark.read
  .option ("header", true)
  .csv (conf.getString ("input.path") )


  /**
   * TRANSFORMATIONS
   */
  logger.info ("=====> Transform data")
  val dfTransformed = df.select (col ("*"), lit (1).alias ("literal") )


  /**
   * OUTPUT
   */
  logger.info ("=====> Writing file")
  //  dfTransformed.write.mode("overwrite")
  //    .csv(conf.getString("output.path"))

//  dfTransformed.write.format("avro")
//    .mode("overwrite")
//    .partitionBy("tr")
//    .save(conf.getString ("output.pathAvro"))

  //  dfTransformed.write.mode("overwrite").parquet(conf.getString("output.pathParquet"))

  logger.info ("=====> sleeping")
  logger.warn ("=====> sleeping")
  logger.error ("=====> sleeping")

  logger.info ("=====> reading avro partition")


//  Thread.sleep (1000000)
}

object executeAvroToParquet{
  /**
   * Spark settings
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

  val dfAvro = spark.read
    .format("avro")
    .load(conf.getString ("output.pathAvro"))
    .where(col("tr") === "01")

  dfAvro.show()

  dfAvro.write.partitionBy("age")
    .parquet(conf.getString ("output.pathParquet"))

}

object executeParquetToParquet{
  /**
   * Spark settings
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

//  val dfParquet = spark.read.parquet(conf.getString ("output.pathParquet")+"/age=22")
//  case class Person(id:String, name:String, age:String, tr:String)
//  import spark.implicits._

  val dfClientes = spark.read
    .parquet(conf.getString ("output.pathParquet"))
    .where(col("age") === "22" )


  dfClientes.show()
  /**
   * TRANSFORMATION
   */
  val dfAgeCount = dfClientes.groupBy("age").count()
  dfAgeCount.show()

  dfAgeCount.write.partitionBy("age")
    .mode("append")
    .parquet(conf.getString("output.pathNewParquet"))

}