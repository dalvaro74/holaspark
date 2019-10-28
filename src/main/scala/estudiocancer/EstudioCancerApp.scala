package estudiocancer

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object EstudioCancerApp {
  //origen dataset: Index of /ml/machine-learning-databases/breast-cancer-wisconsin
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Estudio Cancer")
      .master("local[*]")
      .getOrCreate()

    //Crear una estructura (esquema) para organizar la informacion del dataset del cancer
    // Para el caso de spark-sql debemos trabajar con DF (ROWS). Los DF no trabajan con clases (eso lo hacen los DS),
    //sino con esquemas que se generan a traves de StructType
    val esquemaRegistro = new StructType()
      .add("sample","long")
      .add("cThick","integer")
      .add("uCSize","integer")
      .add("uCShape","integer")
      .add("mAdhes","integer")

    //crear un DF leyendo un CSV pasandole el esquema creado
    val dataSamplesDF = spark.read.format("csv")
      .option("header",false)
      .schema(esquemaRegistro)
      .load("data/breast-cancer-wisconsin.data")
    //dataSamplesDF.show()
    dataSamplesDF.createOrReplaceTempView("cancerTable")
    //Si usamos createOrReplaceGlobalTempView, todas las sesiones de spark que esten abiertas comparten las tablas globales

    val dataSamplesSQL = spark.sql("SELECT sample, uCSize from cancerTable")
    dataSamplesSQL.show()

    //case clases
    //Lo mas probable es que cuando trabajemos con clases estemos tratando con DS en lugar de DF
    //podemos definir una clase dentro de un metodo de una clase

    case class CancerClass(sample:Long, cThick:Int, uCSize:Int, uCShape:Int, mAdhes:Int)

    val cancerDS = spark.sparkContext.textFile("data/breast-cancer-wisconsin.data")
      .map(_.split(",")).map(atributos => CancerClass(atributos(0).trim.toLong,
                                                               atributos(1).trim.toInt,
                                                                atributos(2).trim.toInt,
                                                                 atributos(3).trim.toInt,
                                                                  atributos(4).trim.toInt))
    //cancerDS.

    println("cancerDS es una clase de tipo: "+ cancerDS.getClass)
    import spark.implicits._
    //Aun no es un DS (segun el profe), es un RDD normal y corriente...


    //Crear una funcion embebida en SQL
    def procesar(d: Int): Int = d match { case 2 =>0 case 4 =>1 case _ => -1}
    spark.udf.register("mifuncionSQL",(arg: Int) => procesar(arg))

    val sqlMiFuncion = spark.sql("SELECT *, mifuncionSQL(uCSize) FROM cancerTable")
    sqlMiFuncion.show()

    spark.sparkContext.stop()
  }
}
