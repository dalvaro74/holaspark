import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession
  .builder()
  .appName("DS1-bis")
.master("local[*]")
.getOrCreate()
import spark.implicits._
val dataset1 = spark.sparkContext.parallelize(List(1,2,3,4,5,6)).toDS()
val dataset2 = spark.sparkContext.parallelize(List("a","b","c","d","e","f")).toDS()
//dataset2.show()
val dataJoinDF = dataset1.join(dataset2)
//dataJoinDF.foreach(r => println)
//
val alumnosDF = Seq(
  (1,"Juan Perez",1),
(2,"Maria Perez",1),
(3,"Luis Perez",3),
(4,"Pedro Perez",2),
(5,"Juana Perez",3),
(6,"Teresa Llopis",0)
).toDF("id","alumno","idmodulo")

val modulosBootCampDF = Seq(
  (1,"BD Processing"),
(2,"BD Arquitectura"),
(3,"BD Algebra"),
(4,"BD Machine Learning")
).toDF("id","nombre")


//joinExpression
val joinExpression = alumnosDF.col("idmodulo") === modulosBootCampDF.col("id")
//inner
//val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"inner")
//val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"left_outer")
//val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"right_outer")
//val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"outer")
val alumnosModulosDF = alumnosDF.join(modulosBootCampDF,joinExpression,"cross")
alumnosModulosDF.select($"alumno",$"nombre".as("nombre modulo")).show()


spark.sparkContext.stop()