package tratamientopersonas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

//Vamos a empezar a tratar con DS de SQL-Spark
//Es decir ya no guardamos la info o los datos en Arrays de Tuplas como en el caso de las temperaturas(RDD)
// Ahora almacenamos esa info en case class (DS) o StructType(DF)
object Principal {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("Tratamiento personas")
      .master("local[*]")
      .getOrCreate()

    //Para importar las conversiones implicitas de spark
    import spark.implicits._

    val personas = Seq(Persona("Maria Gomez",34), Persona("Juan Perez",23)).toDS()
    // personas es un objeto de tipo org.apache.spark.sql.Dataset y permite el show que es muy descriptivo (es un println bonito)
    println("personas es una clase de tipo: "+ personas.getClass)
    personas.show

    //Ahora ya no trabajamos con tuplas, trabajamos con clases persona
    personas.map(persona => persona.edad+1).collect().foreach(println)
    //Con esto hemos ejecutado una accion sobre el DS por lo que nos hemos salido del ambito spark/cluster,
    // ya no tenemos un DS sobre el que hacer un show, es decir, con lo de arriba perdemos el DS de personas,
    // solo tendremos los Long de edad

    //Para mantener la clase persona:
    personas.map(persona => persona.copy(edad=persona.edad +1)).collect().foreach(println)

    //O aun mejor, para mantener el DS y seguir trabajando en el cluster (mostramos info que esta alli,
    // no como arriba que nos hemos traido la info a nuestra maquina con el collect y despues la hemos mostrado
    personas.map(persona => persona.copy(edad=persona.edad +1)).show()

    //Hagamoslo con DataFrame
    //Un DF es un caso particular de DS donde el tipo de dato es ROW

    val dataFrame = spark.read.json("data/personas.json")
    println("dataFrame es una clase de tipo: "+ dataFrame.getClass)
    dataFrame.show()
    dataFrame.printSchema()

    /*
    INCISO:
    En el caso del Json, el encabezado o nombre de columna que corresponde a cada dato viene en el propio json
    vamos a ver que pasa si creamos un DF a partir de un csv donde no viene el encabezado.
    Para solucionar la falta de encabezado podemos hacerlo segun se indica en el ejemplo del Estudio del Cancer
    co el StructType()
     */
    val dataFrameCVS = spark.read.csv("data/1800-mini.csv")
    dataFrameCVS.show()
    dataFrameCVS.printSchema()

    //Si quisieramos trabajar con un DS en lugar de con un DF, tenemos que encapsular esta informacion en una clase
    val dataSet = dataFrame.as[Persona]
    println("dataSet es una clase de tipo: "+ dataSet.getClass)
    dataSet.show()
    dataSet.printSchema()



    dataFrame.select($"nombre", $"edad").show()
    //Se puede poner igual sin el $
    //dataFrame.select("nombre", "edad").show()

    //Aqui no se puede quitar el $
    dataFrame.filter($"edad">30).show()

    dataFrame.groupBy("edad").count().show()

    //lanzar queries directamente contra el DS/DF
    dataFrame.createOrReplaceTempView("persona")
    val sql ="SELECT * FROM persona"
    val datosPersonas = spark.sql(sql)
    datosPersonas.show()

    //Si bien esto tambien funciona, es mas seguro e incluso mas eficiente trabajar con el API (es mas facil que nos
    // hackeen o que cometamos erores)

    spark.sparkContext.stop()
  }
}
