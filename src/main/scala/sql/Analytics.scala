package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

object analytics {

  def main(args: Array[String]): Unit = {
    executeCsv
  }

}

object executeCsv {
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

  val df = spark.read
    .option("header", true)
    .csv(conf.getString("input.pathClientes"))


  val dfTransactions = spark.read
    .option("header", true)
    .csv(conf.getString("input.pathTransacciones"))

  /**
   * TRANSFORMATIONS
   */
  logger.info("=====> Transforming table")
  val dfTransformed = df.select(col("*"), lit(1).alias("literal"))

  logger.info("=====> Explain select")
  dfTransformed.explain()

  val dfJoined = dfTransformed.join(
    dfTransactions,
    dfTransformed("tr") === dfTransactions("idTr"),
    "inner"
  )

  logger.info("=====> Explain join")
  dfJoined.explain()

  /**
   * OUTPUT
   */
  logger.info("=====> Writing file")
  dfTransformed.write.mode("overwrite")
    .csv(conf.getString("output.path"))


  logger.info("=====> end process")
  logger.warn("=====> sleeping")

//  Thread.sleep(10000000)
}
