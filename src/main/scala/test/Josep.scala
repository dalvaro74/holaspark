package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Josep {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Ejemplo Josep Sack")
      .master("local[*]")
      .getOrCreate()

    //Para importar las conversiones implicitas de spark
    import spark.implicits._
    /*
    Este ejemplo lo puso Josep con referencia a la practica, pero no hay streaming por ningun lado
    Aun asi lo escribo como ejemplo y para comprobar que funciona
    Vamos a intentar hacer lo mismo con RDD y DS
     */

    val inputDS = spark.read
      .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/mok_data_sin_time.csv")
    .as[Messages]

    // Lo pasmos a un RDD para trabajar con sus funciones correspondientes tal y como lo tiene Josep
    val InputDSFiltrado = inputDS.select($"message").filter($"user_id" > 0).rdd
    // Convertimos las frases o mensajes a String para poder aplicarles el split y separarlas en palabras mdiante
    // el flatmap. Ademas obviamos las palabras de menos de 3 letras
    val palabras = InputDSFiltrado.map(frase => frase.toString()).flatMap(palabras2 => palabras2.split("\\W+"))
    .map(palabra => palabra.toLowerCase).filter(tipoPal => tipoPal.length > 3)
    // Convertimos cada palabra en una tupla (palabra,1) y con el reduceByKey agrupamos todas las palabras que son
    // iguales y sumamos sus "unos" lo que nos dara el numero de veces que está repetida esa palabra.
    val contadorPalabras = palabras.map(palabraTL => (palabraTL,1)).reduceByKey((a,b) => a + b)
    // por ultimo las ordenamos, para ello primero le damos la vuelta a la tupla con el metodo swap para que el numero
    // de repeticiones esté primero y a continuacion ordenaos por la key (que es el primer elemento de la tupla)
    // Cogiendo los 20 primeros elementos con el take
    val contadorPalabrasOrdenado = contadorPalabras.map(par => par.swap).sortByKey(false).take(20)
    contadorPalabrasOrdenado.foreach(println)

    //Vamos a intentar lo mismo pero trabajando directamente sobre los DS
    //Primero me quedo solo con la columna message
    val contadorPalabrasOrdenado2 = inputDS.select($"message")
      .map(frase => frase.toString())
      .flatMap(frase => frase.split("\\W+"))
      .filter(_.length >3)
      .groupBy("value")
      .count().
      orderBy($"count".desc).
      limit(10)
    contadorPalabrasOrdenado2.show()

    /*
    val InputDSFiltrado2 = inputDS.select("message")
      .groupBy("message")
      .count().orderBy("count")
      .take(20)*/

    //InputDSFiltrado2.foreach(println)

    spark.sparkContext.stop()
  }

}
