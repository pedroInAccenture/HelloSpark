package sql


  import com.typesafe.config.Config
  import org.apache.log4j.Logger
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, lit}
  import utils.{Constant, LoadConf}


 object Particion extends App {


    val logger = Logger.getLogger(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("example")
      .getOrCreate()


    val conf: Config = LoadConf.getConfig


    logger.info("=====> Reading file")

    val data = spark.read
      .option("header", true)
      .csv(conf.getString("input.pathDos"))


    val dataTransformed = data.select(col("*"), lit("2").as("hobi"))


    logger.info("=====> Writing file") //log
    //particion
    dataTransformed.write.partitionBy("tr").format("avro").mode("overwrite")
      .save(conf.getString("output.pathAvro"))

    dataTransformed.write.partitionBy("tr").format("parquet").mode("overwrite")
      .save(conf.getString("output.pathParquet"))


    logger.info("=====> sleeping")

  }
