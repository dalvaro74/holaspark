package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object ContinuousApp {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Analisis RT Rate")
      .master("local[*]")
      .getOrCreate()

    val data = spark.readStream
                .format("rate")
                .option("rowsPerSecond",10)
                .load()

    val query = data.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 second"))
      .start()
      .awaitTermination()

  }
}
