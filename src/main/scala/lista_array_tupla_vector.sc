//Hay una clase Tupla para cada tamaño hasta Tupla22,
//Si se necesita almacenar mas elementos debemos recurrir a una coleccion (Lista, Array, Vector)
//Inmutables
val miLista = List(1,2,3)
val miVector = Vector(1,2,3)
val miTupla = Tuple3(1,2,3)
// Es equivalente a
//miTupla = (1,2,3)

//Mutables
val miArray = Array(1,2,3)

println("miLista es una clase de tipo: "+ miLista.getClass)
println("miTupla es una clase de tipo: "+ miTupla.getClass)
println("miVector es una clase de tipo: "+ miVector.getClass)
val nose = (1,2,3)
println("nose es una clase de tipo: "+ nose.getClass)

//val nose2 = (1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3,1,2,3)
// No esta permitido porque tiene mas de 22 elementos y no lo puede convertir en Tupla automaticamete, tendras que decirle
// explicitamente si es una lista, un Array u otra coleccion

//Para acceder al primer elemento  de una tupla no podemos usar los (), se usa el ._ ademas el primer elemento es el 1
// no el cero como el Arrays, Vectores , listas y todos los lenguajes de programacion conocidos del mundo mundial
println(miTupla._1)
println(miVector(0))
println(miLista(0))
println(miArray(0))





//Las listas, las tuplas y los vectores son inmutables, no se pueden modificar sus elementos, ya que no tienen la funcion update
//Estas expresiones no funcionan
//miTupla(0)="0"
//miLista(0)="0"
//miVector(0)=0

miArray(0)=0
