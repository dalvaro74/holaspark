//case clases son como clases ligeras (solo tiene atributos, es decir estado, no comportamiento (metodos), aunque si podria
// tenerlos, pero no tendria mucho sentido)
case class Usuario(nombre:String,apellido:String, edad:Int)

val juan = Usuario("Juan","Palomo",34)
val maria = juan.copy(nombre = "Maria")

println(s"Hola, me llamo ${juan.nombre} ${juan.apellido}")
println(s"Hola, me llamo ${maria.nombre} ${maria.apellido}")

//El case class ofrece un println muy claro, a diferencia del class como se ve abajo que la informacion
// que presenta el println no es util
println(juan)


class Coche(val marca: String)

val miSeat = new Coche("Seat")

println(miSeat)


//Object ...
//Singleton
object Logger {
  def log(mensaje: String) = {
    println(mensaje)
  }
}

Logger.log("Mensaje a consola")
//proceso
Logger.log("Otro mensaje a consola...")

//pattern matching
val dato = 0

val resultado = dato match {
  case 0 => {
              var c = 1
              val a = 1 + c
              s"Sin valores ${a}"
            }
  case 1 => "Valor unico"
  case 2 => "Valor doble"
  case _ => "Cualquier otro caso"
}


println(s"Resultado:${resultado}")

println({
  var x = 0
  x = x + 1
  x
  "Sin valores"
})


def procesar(dato: Int): String = dato match {
  case 0 => "Sin datos"
  case _ => "Hay valores"
}

val cadena = procesar(0)
println(s"Cadena:${cadena}")



