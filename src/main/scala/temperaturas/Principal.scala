package temperaturas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Principal {

  //def parseainformacion(linea:String): (String,String,String,String) = {}
  //No hace falta incluir el formato de salida
  def parseaInformacion(linea:String) = {
    val info = linea.split(",")
    val estacion = info(0)
    val tipoMedicion = info(2)
    val temperatura = info(3).toFloat * 0.1f * (9.0f / 5.0f)

    // las funciones devuelven la ultima expresion
    (estacion, tipoMedicion, temperatura)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Analisis temperaturas")
      .master("local[*]")
      .getOrCreate()

    val data = spark.sparkContext.textFile("data/1800.csv")
    //Este data es un RDD
    //data.take(20).foreach(println)
    val infoTratada = data.map(parseaInformacion)
    //infoTratada sigue siendo un RDD seria como una especia de array de Tuplas3 ->(estacion, tipoMedicion, temperatura)
    println("infoTratada es una clase de tipo: "+ infoTratada.getClass)
    //infoTratada.foreach(println)

    //Vamos a sacar las temperaturas mimimas de cada estacion
    val infoTempMinima = infoTratada.filter(medicion => medicion._2 == "TMIN")

    val estacionesTemperatura = infoTempMinima.map(medicion => (medicion._1,medicion._3.toFloat))

    val estacionTempMinima = estacionesTemperatura.reduceByKey((a,b) => Math.min(a,b))

    estacionTempMinima.collect().foreach(println)



    spark.sparkContext.stop()
  }
}
