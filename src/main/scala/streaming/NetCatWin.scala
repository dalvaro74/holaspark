package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp
import  org.apache.spark.sql.functions._

object NetCatWin {
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
      .option("includeTimeStamp",true)
      .load()
    import spark.implicits._


    //proceso
    //Ya no trabajamos con cadenas sino con tuplas
    val palabras = info.as[(String, Timestamp)].flatMap(par => par._1.split(" ")
      .map(palabra => (palabra, par._2)))
      .toDF("palabra","timestamp")


    //ventana de tiempo
    val conteoPalabrasCada10seg = palabras.groupBy(window($"timestamp", "10 seconds","5 seconds"),$"palabra")
      .count()
      .orderBy("window")

    println("info es una clase de tipo: "+ info.getClass)
    println("palabras es una clase de tipo: "+ palabras.getClass)
    println("conteoPalabrasCada10seg es una clase de tipo: "+ conteoPalabrasCada10seg.getClass)

    val query = conteoPalabrasCada10seg.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    println("query es una clase de tipo: "+ query.getClass)
    query.awaitTermination()



    spark.sparkContext.stop()

  }

}
