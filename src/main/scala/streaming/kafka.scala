package streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object kafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Kafka Spark WC")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val data = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","bigdata")
      //.option("includeTimeStamp",true)
      .load()
      //.selectExpr("CAST(value AS STRING)")
      .selectExpr("CAST(value AS STRING) AS Mensaje","timestamp as Tiempo")
      .as[String]

    //Asi era incialmente
    //val conteoPalabras = data.flatMap(entrada => entrada.split(" ")).groupBy("value").count()


    println("data es una clase de tipo: "+ data.getClass)
    println("conteoPalabras es una clase de tipo: "+ conteoPalabras.getClass)

    val query = conteoPalabras.writeStream
      .outputMode("update")
      .format("console")
      .option("checkpointLocation","checkpoint")
      .start()
      .awaitTermination()

    println("query es una clase de tipo: "+ query.getClass)
    spark.sparkContext.stop()

  }

}