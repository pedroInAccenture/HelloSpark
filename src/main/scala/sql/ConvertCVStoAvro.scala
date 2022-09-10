package sql
import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import utils.{Constant, LoadConf}

//Convertir archivo csv a archivo AVRO
object ConvertCVStoAvro extends App{
  /**
   * Spark settings
   * cargar la configuracion del spark junto con el logger
   *
   */
  //bibliotecas que ayudan a generar logs
  val logger = Logger.getLogger(this.getClass.getName)

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()


  /**
   * PARAMETERS
   */
  //cargar configuracion apartir de archivos de configuracion (en resources .conf)
  val conf: Config = LoadConf.getConfig //cargan los parametros apartir del archivo


  /**
   * INPUTS
   */
  logger.info("=====> Reading file") //llevar la traza
  //crear dataframe desde un archivo csv
  //leer archivos csv
  val data = spark.read
    .option("header",true) //para que se muestre el header
    .csv(conf.getString("input.pathDos")) //se va a cargar de esa ruta
   // df.show() //se imprime la tabla
   // df.printSchema() //se imprime la tabla

  /**
   * TRANSFORMATIONS
   */
  val dataTransformed = data.select(col("*"),lit("3").as("literal"))
  //se hace un select, todas las columnas y se agrega columna extra

  /**
   * INPUTS
   */

  logger.info("=====> Writing file") //log
  //convertir csv a avro
   dataTransformed.write.format("avro").mode(SaveMode.Overwrite)
  .save(conf.getString("output.pathDos")) //donde se va a guardar el archivo



  logger.info("=====> sleeping")


}
