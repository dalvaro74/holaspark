import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Testing")
  .master("local[*]")
  .getOrCreate()


import spark.implicits._

val listaCadenas = List("Hola no tengas en cuenta que estamos en una prueba de concepto",
  "Esta es la seguna frase y no las tengo todas conmigo")

val listaDS = listaCadenas.toDS()
listaDS.show()

val palabrasMap = listaDS.map(cad => cad.split(" "))
palabrasMap.show()

val palabrasFlat = listaDS.flatMap(cad => cad.split(" "))
palabrasFlat.show()


spark.sparkContext.stop()