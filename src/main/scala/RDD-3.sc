import org.apache.spark.sql.SparkSession

//Vamos a ver la Union y la Interseccion de RDDs
//levantamos un SparkSession
val spark = SparkSession
  .builder()
  .appName("Animales app RDD")
  .master("local[*]")
  .getOrCreate()

val developerScalaRDD = spark.sparkContext.parallelize(List("vision","paradigma funcional","paradigma poo","mutabilidad","inmutabilidad"))
val developerSparkRDD = spark.sparkContext.parallelize(List("vision","paradigma funcional","paradigma poo"))

val developerBigDataSkillsRDD = developerScalaRDD.intersection(developerSparkRDD)
val developerScalaYSparkSkillsRDD = developerScalaRDD.union(developerSparkRDD)

//iteracion sobre estructuraRDD de cluster (de spark)
developerBigDataSkillsRDD.foreach(println)
//iteracion sobre estructura local (de scala)
developerScalaYSparkSkillsRDD.collect().foreach(println)

//filtrar del RDD aquellas scala developer skill que tengan dos o mas palabras en su descripcion

//Dani
//val mas2PalabrasRDD = developerScalaRDD.filter(palabra => palabra.split(" ").size >=2).collect()
//mas2PalabrasRDD.foreach(println)

//Profe
val mas2PalabrasRDD = developerScalaRDD.filter(skill => skill.split(" ").size > 1)

developerScalaRDD.foreach(println)
println("***************")
mas2PalabrasRDD.foreach(println)

spark.sparkContext.stop()