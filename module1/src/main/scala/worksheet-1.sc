def sayHello(name:String)(implicit myself: String) = {
  s"Hello $name! My name is ${myself}"
}

implicit val myString = "implicits"
val fast = sayHello("test")   // single param passed implicit assumed by default
println(fast)