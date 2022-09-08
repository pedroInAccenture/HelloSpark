package sql
  import com.typesafe.config.Config
  import org.apache.log4j.Logger
  import org.apache.spark.sql.{SaveMode, SparkSession}
  import org.apache.spark.sql.functions.{col, lit}
  import utils.{Constant, LoadConf}


  object ParquetToParquet extends App{


    val logger = Logger.getLogger(this.getClass.getName)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("example")
      .getOrCreate()



    val conf: Config = LoadConf.getConfig


    logger.info("=====> Reading file")
    val data = spark.read
      .format("parquet")
      .load(conf.getString("output.pathTres"))

    val dataTransformed = data.select(col("*"), lit("4").as("naranja"))

    logger.info("=====> Writing file")
    dataTransformed.write.format("parquet").mode(SaveMode.Overwrite)
      .save(conf.getString("output.pathCuatro"))
    logger.info("=====> sleeping")
  }

