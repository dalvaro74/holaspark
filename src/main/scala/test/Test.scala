package test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Tratamiento personas")
      .master("local[*]")
      .getOrCreate()

    //Para importar las conversiones implicitas de spark
    import spark.implicits._

    //A PARTIR DE AQUI IRA LO QUE QUERAMOS PROBAR!!

    val dataset2 = spark.sparkContext.parallelize(List("a","b","c","d","e","f")).toDS()
    dataset2.show()

    //Con esto podemos comprobar la diferencia entre el Map y el FlatMap
    val listaCadenas = List("Hola no tengas en cuenta que estamos en una prueba de concepto",
      "Esta es la seguna frase y no las tengo todas conmigo")

    val listaDS = listaCadenas.toDS()
    listaDS.show()

    val palabrasMap = listaDS.map(cad => cad.split(" "))
    palabrasMap.show()

    val palabrasFlat = listaDS.flatMap(cad => cad.split(" "))
    palabrasFlat.show()


    val articulos = Seq(Articulo("Pedro","BigData",1),
      Articulo("Maria","Spark Basics",2),
      Articulo("Luis","Kafka Basics",3)).toDS()

    val visionadoDeArticulos = Seq(VisionadoArticulo(1,100),
      VisionadoArticulo(2,20),
      VisionadoArticulo(3,400)).toDS()

    //val report = articulos.joinWith(visionados,articulos("id") === visionados("articulo"), "inner")
    val report = articulos.joinWith(visionadoDeArticulos,articulos("id") === visionadoDeArticulos("articulo"), "left")
    report.show()

    //Esto debemos dejarlo siempre...
    spark.sparkContext.stop()


  }
}
