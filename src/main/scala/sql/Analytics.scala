package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
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
    .csv(conf.getString("input.path")) //Lee el archivo del conf el de clientes csv


  /**
   * TRANSFORMATIONS
   */
  val dfTransformed = df.select(col("*"),lit(3))


  /**
   * INPUTS
   */
  logger.info("=====> Writing file")

  dfTransformed.write.mode("overwrite")
    .csv(conf.getString("output.path")) //Para que se guarde en el de clientes


  logger.info("=====> sleeping")
//  Thread.sleep(1000000)

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

