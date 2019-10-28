package sparkSQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Sparksql {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Contador Quijote")
      .master("local[*]")
      .getOrCreate()






    spark.sparkContext.stop()
  }
}
