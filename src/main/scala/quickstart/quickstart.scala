package quickstart

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object Quickstart extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder()
    .appName("Dani")
    .master("local[*]")
    .getOrCreate()

  //Para un fichero generico
  //val textFileSD = spark.read.textFile("data/donquijote.txt")

  //Un csv puede ser tratado como un fichero generico
  val textFileSD = spark.read.textFile("data/airbnb-mini.csv")

  // O como  un csv (con ; como separador)
  //val csvFileSD = spark.read.option("sep",";").csv("data/airbnb-mini.csv")
  //De manera generica
  //val csvFileSD = spark.read.format("csv").option("header", "true").option("sep", ";").load("data/airbnb-mini.csv")
  //Tambien valdria
  val csvFileSD = spark.read
          .option("header", "true")
          .option("sep", ";")
          .csv("data/airbnb-mini.csv")


  //Vamos a contar las palabras del Quijote usando DS
  val quijoteSD = spark.read.textFile("data/donquijote.txt")
  import spark.implicits._
  val numPalabras = quijoteSD.map(line => line.split(" ").size).reduce((a, b) => a+b)
  println(numPalabras)

  //Para obtener un dataset con todas las palabras
  val wordsQuijote = quijoteSD.flatMap(linea => linea.split("\\W+"))
  println(wordsQuijote.count())
  wordsQuijote.show()

  //Si quisieramos que las palabras fueran en mayusculas
  val wordsQuijoteUper = quijoteSD.flatMap(linea => linea.split(" ").map(lin=>lin.toUpperCase()))
  wordsQuijoteUper.show()

  //Para obtener un dataset con todas las palabras unicas
  val wordsQuijoteDistinct = quijoteSD.flatMap(linea => linea.split("\\W+")).groupByKey(identity)
  //Lo de arriba es un objeto de la clase spark.sql.KeyValueGroupedDataset aplicandole el count() volvemos a tener un dataset
  val wordsQuijoteDistinctDS = wordsQuijoteDistinct.count()
  wordsQuijoteDistinctDS.show()

  //println("wordsQuijoteDistinct es una clase de tipo: " + wordsQuijoteDistinct.getClass)




  /*
  val quijoteSD = spark.read.textFile("data/donquijote.txt")
  val numPalabras = quijoteSD.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)

  //Vamos a filtrar las lineas que contengan un determinado string
  val miString = "Loft"
  //import spark.implicits._
  val lineasConString = textFileSD.filter(line => line.contains("Loft"))
  //println(textFileSD.count())
  //println(textFileSD.first())
  //textFileSD.take(2).foreach(println)
  textFileSD.show()
*/
  spark.sparkContext.stop()

}
