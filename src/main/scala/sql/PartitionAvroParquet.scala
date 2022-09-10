package sql

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.LoadConf
/*
*
* Particionar la tabla clientes por el campo tr
* Partionar avro y parquet por la tabla clientes
*
*
*  */
object PartitionAvroParquet extends App{
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

  //leer archivos csv
  val data = spark.read
    .option("header", true)
    .csv(conf.getString("input.path")) //toma archivo cvs clientes


  /**
   * TRANSFORMATIONS
   */

  val dataTransformed = data.select(col("*"), lit("6").as("columna"))

  /**
   * INPUTS
   */

  logger.info("=====> Writing file") //log

  //particion
  dataTransformed.write.partitionBy("tr").format("avro").mode("overwrite")
    .save(conf.getString("output.pathAvroC"))

  dataTransformed.write.partitionBy("tr").format("parquet").mode("overwrite")
   .save(conf.getString("output.pathParquet"))


  logger.info("=====> sleeping") //termina

}
