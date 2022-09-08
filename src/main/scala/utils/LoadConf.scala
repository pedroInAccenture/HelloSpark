package utils

import com.typesafe.config.{Config, ConfigFactory}

object LoadConf {
  //declaracion de 3 metodos

  def input():Config = getConfig.getConfig("input") //traen un objeto tipo Config en memoria

  def output():Config = getConfig.getConfig("output")
  //se leera del archivo config y lee el primer nivel
  def getConfig:Config = ConfigFactory.load("config/application.conf").getConfig("app")

}
