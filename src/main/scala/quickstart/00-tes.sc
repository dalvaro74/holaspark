import org.apache.spark.sql.SparkSession


//levantamos un SparkSession
val spark = SparkSession
  .builder()
  .appName("Tets")
  .master("local[*]")
  .getOrCreate()

val listaAnimales = List("Perro","Gato","Tigre","Caballo","Camello","Perro")


//Vaos a generar un RDD de Spark a partir de esta lista
val animalesRDD = spark.sparkContext.parallelize(listaAnimales)
//animalesRDD.take(3).foreach(println)
animalesRDD.foreach(println)