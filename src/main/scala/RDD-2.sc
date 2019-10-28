import org.apache.spark.sql.SparkSession

//levantamos un SparkSession
val spark = SparkSession
  .builder()
  .appName("Animales app RDD")
  .master("local[*]")
  .getOrCreate()

//groupByKey -> Tuplas -> Duplas -> RDD + Duplas ->Pair RDD -> (key,value)

val pacientes = List(("11111111H",39.1),("22222222H",34.2),("11111111H",39.6),("33333333H",37.4),("22222222H",36.5),("11111111H",40.5))
//Con el parallelize pasamos una Lista de Scala al ambito de Spark (cluster) transformandolo en un RDD
//de tipo ParallelCollectionRDD
val pacientesRDD = spark.sparkContext.parallelize(pacientes) //RDD
println("pacientesRDD es una clase de tipo: "+ pacientesRDD.getClass)


//GroupByKey
/*
("11111111H",[39.1,39.6]) -> ("11111111H",(39.1+39.6))
("22222222H",[34.2,36.5])
("33333333H",[37.4])
* */
val valoresGroupByKey = pacientesRDD.groupByKey()
//groupByKey convierte el RDD en un ShuffledRDD
println("valoresGroupByKey es una clase de tipo: "+ valoresGroupByKey.getClass)
valoresGroupByKey.foreach(println)

val valores = valoresGroupByKey .map(paciente => (paciente._1,paciente._2.sum))
//paciente._2 es un vector
//map convierte el RDD en un MapPartitionsRDD
println("valores es una clase de tipo: "+ valores.getClass)
println("La suma con GroupByKey")
valores.foreach(println)

//ReducebyKey
//Para calcular la suma
val valoresReduceByKey = pacientesRDD.reduceByKey((a,b) => a + b)
println("valoresReduceByKey es una clase de tipo: "+ valoresReduceByKey.getClass)
//En notacion reducida:  valoresReduce = pacientesRDD.reduceByKey(_+_)
//reduceByKey convierte el RDD en un ShuffledRDD
val miCollect = valoresReduceByKey.collect()
//Este collect devuelve una Tuple2
println("La suma con ReducebyKey")
miCollect.foreach(println)

//Para calcular la temperatura media
val totalPacientes0 = pacientesRDD.mapValues(x => (x,1))
//totalPacientes0.foreach(println)
val totalPacientes = totalPacientes0.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
val media = totalPacientes.mapValues(x => x._1/x._2)
println("La Media de las temperaturas por paciente:")
//media.foreach(println)
//En un unico paso
pacientesRDD.mapValues(x => (x,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues(x => x._1/x._2).foreach(println)


// Como vemos tanto GROUPBYKEY como REDUCEKEY hacen lo mismo en este
// caso por lo que deberiamos investigar cual de las dos es mas
//eficiente para este caso.

spark.sparkContext.stop()