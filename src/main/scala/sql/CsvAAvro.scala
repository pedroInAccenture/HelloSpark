package sql

  import com.typesafe.config.Config
  import org.apache.log4j.Logger
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, lit}
  import utils.{Constant, LoadConf}

  object CsvAAvro extends App {


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



    val dataTransformed = data.select(col("*"), lit("3").as("literal"))


    logger.info("=====> Writing file")

    dataTransformed.write.format("avro").mode(SaveMode.Overwrite)
      .save(conf.getString("output.pathDos"))
    logger.info("=====> sleeping")
  }


