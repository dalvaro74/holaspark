package quixote

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import util._

object Principal {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contador Quijote")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    //leer el quijote
    //RDD -> Resilient Distributed Dataset (memoria)
    //val inputRDD = spark.sparkContext.textFile("data/donquijote.txt")
    // In Spanish
    val inputRDD = spark.sparkContext.textFile("data/quijote-spanish.txt")
    //Input es un RDD de Spark (org.apache.spark.rdd.MapPartitionsRDD)
    println("inputRDD es una clase de tipo: " + inputRDD.getClass + " (representa las frases del quijote)")
    println("inputRDD SIZE: "+ inputRDD.count())
    //spark es lazy eval

    //transformaciones
    //"Sancho panza cogio su caballo" -> ["Sancho","panza","cogio"]
    //DAG -> Direct Acyclic Graph
    //Cogemos como separador del split cualquier cosa que no sea alfanumerica (por ejemplo ", "), para evitar
    //las palabraas con comas o puntos que nos salian cuando el separador era simplemnete el espacio.
    val palabras = inputRDD.flatMap(frase => frase.split("\\W+"))
    //val palabrasMinusculas = palabras.map(palabra => palabra.toLowerCase)
    //Mejor asi:
    val palabrasMinusculas = palabras.map(_.toLowerCase)

    println("palabrasMinusculas es una clase de tipo: "+ palabrasMinusculas.getClass)
    println("palabrasMinusculas SIZE: " + palabrasMinusculas.count() + " (representa todas las palabras del quijote)")


    //Accion profe
    //El reduceByKey convierte nuestro objeto palabrasMinusculas de tipo "org.apache.spark.rdd.MapPartitionsRDD"
    // en el objeto contadorPalabras de tipo org.apache.spark.rdd.ShuffledRDD
    val contadorPalabras = palabrasMinusculas.map(palabra => (palabra,1)).reduceByKey((a,b) => a + b)
    println("contadorPalabras es una clase de tipo: "+ contadorPalabras.getClass)
    println("contadorPalabras SIZE: "+ contadorPalabras.count() + " (representa las palabras del quijote sin repetir)")

    val contadorPalabrasOrdenado = contadorPalabras.map(par => (par._2,par._1)).sortByKey(false)
    println("contadorPalabrasOrdenado es una clase de tipo: "+ contadorPalabrasOrdenado.getClass)
    println("contadorPalabrasOrdenado SIZE: "+ contadorPalabrasOrdenado.count() )

    //Para guardar estos datos directamente en el cluster...
    //En nuestro caso un clustr simulado (se crea un nuevo directorio llamado output
    //Ademas lo guarda particionado, como corresponde a un cluster de Spark/Hadoop
    //Con coalesce podemos indicarle en cuantas particiones lo separa
    contadorPalabrasOrdenado.coalesce(1).saveAsTextFile("output")

    //Con el take ya pasamos del ambito spark: ShuffledRDD al ambito scala: Tupla2
    val CienPrimerasOrdenadas = contadorPalabrasOrdenado.take(100)
    println("CienPrimerasOrdenadas es una clase de tipo: "+ CienPrimerasOrdenadas.getClass)

    //veintePrimerasOrdenadas.foreach(println)

    println("***********")
    //Fran
    // Acciones Fran
    //Esto saca palabras del ambito Spark (RDD) y lo lleva al ambito scala (HashMap)
    val contadorPalabras2 = palabras.map(_.toLowerCase).countByValue()
    println("contadorPalabras2 es una clase de tipo: "+ contadorPalabras2.getClass)
    println("contadorPalabras2 SIZE: "+ contadorPalabras2.size)
    //contadorPalabras2.foreach(println)


    println("***********")
    //acciones Inciales con collect
    //Collect devuelve un Array de Strings
    val contadorPalabras3 = palabras.collect()
    println("contadorPalabras3 es una clase de tipo: "+ contadorPalabras3.getClass)
    println("contadorPalabras3 SIZE: "+ contadorPalabras3.length)


    spark.sparkContext.stop()

  }
}

