
import org.apache.spark.sql.SparkSession

//levantamos un SparkSession
val spark = SparkSession
  .builder()
  .appName("Animales app RDD")
  .master("local[*]")
  .getOrCreate()

val listaAnimales = List("Perro","Gato","Tigre","Caballo","Camello","Perro")


//Vaos a generar un RDD de Spark a partir de esta lista
val animalesRDD = spark.sparkContext.parallelize(listaAnimales)

val valores = animalesRDD.countByValue()
valores.foreach(println)

//todas las transformaciones de un RDD son lazy,
// es decir no se ejecutan hasta que lleva a cabo una accion sobre el RDD
val animalesSinRepeticionesSinPerros = animalesRDD.distinct().filter(animal => animal != "Perro").collect()


for(animal <- animalesSinRepeticionesSinPerros) {
  println(animal)
}

// BroadCast variable
//Vamos a levantar una coleccion
val numerosAlmacen = spark.sparkContext.parallelize(Seq(1,2,3))
val factorRedondeo = 0.5

//Esto no se puede usar ya que factorRedondeo es una variable local, debemos hacerla distribuida
//para ser usada en el parallelice es decir en el cluster
//numerosAlmacen.map(numero => numero + factorRedondeo)

//Para que esta variable pueda ser usada en el cluster:
val factorRedondeoDistribuido = spark.sparkContext.broadcast(factorRedondeo)
numerosAlmacen.map(numero => numero + factorRedondeoDistribuido.value).foreach(println)

spark.sparkContext.stop()








