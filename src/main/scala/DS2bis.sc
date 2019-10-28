// Datasets

import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Analisis temperaturas")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

case class Articulo(author: String, titulo: String, id: Int)
case class VisionadoArticulo(articulo: Int, numVisionados: Int)

val articulos = Seq(Articulo("Pedro","BigData",1),
  Articulo("Maria","Spark Basics",2),
  Articulo("Luis","Kafka Basics",3)
).toDS()

val visionadoDeArticulos = Seq(VisionadoArticulo(1,100),
  VisionadoArticulo(2,20),
  VisionadoArticulo(3,400)).toDS()

//val report = articulos.joinWith(visionados,articulos("id") === visionados("articulo"), "inner")
val report = articulos.joinWith(visionadoDeArticulos,articulos("id") === visionadoDeArticulos("articulo"), "left")
//report.show()
spark.sparkContext.stop()