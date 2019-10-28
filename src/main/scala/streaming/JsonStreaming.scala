package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object JsonStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Monitrizacion JSON")
      .master("local[*]")
      .getOrCreate()

    val esquema = new  StructType()
                      .add("nombre","string")
                      .add("edad","integer")


    val input = spark.readStream
                        .format("json")
                        .schema(esquema)
                        .load("data")

    //proceso
    import spark.implicits._
    val resultadoProceso = input.select($"nombre")

    val query = resultadoProceso.writeStream
                  .format("parquet")
                  .option("checkpointLocation","checkpoint")
                  .start("output")
                  .awaitTermination()
  }
}
