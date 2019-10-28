package varios
/*
object Arguments {
  def main(args: Array[String]): Unit = {
    args.foreach(println)
  }

  }
*/
//Podemos hacer lo mismo que arriba extendiendo directamente el objeto de App
object Arguments extends App{
  args.foreach(println)
}
