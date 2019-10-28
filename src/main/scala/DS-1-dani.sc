import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("DS1")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
val listaAnimales = List("Perro","Gato","Tigre","Caballo","Camello","Perro")
val listaNumeros = List(1,2,3,4,5,6)
val listaLetras = List("a","b","c","d","e","f")
val dataSet1 = listaAnimales.toDS()
val dataSet2 = listaLetras.toDS()

val dataJoinDF = dataSet1.join(dataSet2)

//dataJoinDF.foreach(r=>println)
//Esto no funciona

val alumnosDF = Seq(
    (1, "Juan Perez",1),
    (2, "Maria Perez",1),
    (3, "Luis Perez",2),
    (4, "Pedro Perez",3),
    (5, "Juana Perez",3)
).toDF("id","alumno","idmodulo")

val modulosBootCampDF = Seq(
  (1, "BD Procesing"),
  (2, "BD Arquitectura"),
  (3, "BD Algebra")
).toDF("id","nombre")

//inner
//joinexpresion
val joinExpression = alumnosDF.col("idmodulo") === modulosBootCampDF.col("id")

val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"inner")

//alumnosModulosDF.select($"alumno",$"nombre").collect().foreach(r=>println)
alumnosModulosDF.select($"alumno",$"nombre".as("nombre modulo")).show()