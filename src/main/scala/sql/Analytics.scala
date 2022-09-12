package sql


import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit} //blibliotecas de spark sql
import utils.{Constant, LoadConf}

object Analytics extends App {

  /**
    *  Spark settings
   *  cargar la configuracion del spark junto con el logger
   *
   */
    //bibliotecas que ayudan a generar logs
  val logger = Logger.getLogger(this.getClass.getName)

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()


  /**
    *  PARAMETERS
   */
    //cargar configuracion apartir de archivos de configuracion (en resources .conf)
  val conf:Config = LoadConf.getConfig //cargan los parametros apartir del archivo


  /**
    * INPUTS
   */
  logger.info("=====> Reading file") //llevar la traza
 //leer archivos csv
  val df = spark.read
    .option("header",true)
    .csv(conf.getString("input.path")) //se va a cargar de esa ruta
  df.show() //se imprime la tabla
  df.printSchema()  //se imprime la tabla

  /**
   * TRANSFORMATIONS
   */

  val dfTransformed = df.select(col("*"),lit(1))



  /**
   * INPUTS
   */
  //escribir el archivo
  logger.info("=====> Writing file")
  dfTransformed.write.mode("overwrite")
    .csv(conf.getString("output.path"))


  logger.info("=====> sleeping")
//  Thread.sleep(1000000)


}
//Prueba Git


