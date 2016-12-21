def sayHello(name:String)(implicit whoAreYou: () => String) = {
  s"Hello $name! My name is ${whoAreYou()}"
}

implicit def provideName() = {"Scala"}
implicit val myString = "implicits"
val fast = sayHello("test")   // single param passed implicit assumed by default
println(fast)