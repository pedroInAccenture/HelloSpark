package sql


  import com.typesafe.config.Config
  import org.apache.log4j.Logger
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, lit}
  import utils.{Constant, LoadConf}



  object AvroToParquet extends App{


    val logger = Logger.getLogger(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("example")
      .getOrCreate()



    val conf: Config = LoadConf.getConfig


    logger.info("=====> Reading file")
    val data = spark.read
      .format("avro")
      .load(conf.getString("output.pathDos"))

    val dataTransformed = data.select(col("*"), lit("7").as("dado"))

    logger.info("=====> Writing file")
    dataTransformed.write.format("parquet").mode(SaveMode.Overwrite)
      .save(conf.getString("output.pathTres"))
    logger.info("=====> sleeping")
  }

