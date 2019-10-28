import org.apache.spark.sql._

//levantamos un SparkSession
val spark = SparkSession
  .builder()
  .appName("Tets")
  .master("local[*]")
  .getOrCreate()

//Con esto obtenemos un RDD, pero yo quiero un DS
//val textFileRDD = spark.sparkContext.textFile("data/airbnb-listings-Madrid.csv")

//En los worksheets la ruta relativa no es valida hay que poner la absoluta (En los RDDs
// no canta hasta que vas a usar la variable generada en DS si
import spark.implicits._
val textFileSD = spark.read.textFile("C:\\Users\\Texelia\\IdeaProjects\\holaspark\\data\\1800-mini.csv")

textFileSD.count()
import spark.implicits._

val listaCadenas = List("Hola no tengas en cuenta que estamos en una prueba de concepto",
  "Esta es la seguna frase y no las tengo todas conmigo")

val listaDS = listaCadenas.toDS()
listaDS.show()

spark.sparkContext.stop()