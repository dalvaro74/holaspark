package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object NetCat {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("NetCat")
      .master("local[*]")
      .getOrCreate()

    val info = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9000)
      .load()
    import spark.implicits._

    //proceso
    val conteoPalabras = info.as[String].flatMap(cadena => cadena.split(" ")).groupBy("value").count()
    //val conteoPalabras = data.flatMap(entrada => entrada.split(" ")).groupBy("value").count()

    println("info es una clase de tipo: "+ info.getClass)
    println("conteoPalabras es una clase de tipo: "+ conteoPalabras.getClass)

    val query = conteoPalabras.writeStream
      .outputMode(("update"))
      .format("console")
      .start()

    println("query es una clase de tipo: "+ query.getClass)

    query.awaitTermination()

    spark.sparkContext.stop()
  }

}
