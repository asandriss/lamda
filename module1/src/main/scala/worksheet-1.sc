object HelloWorld {
  def main(args: Array[String]): Unit = {
    val fast = sayHello("test")(provideName)

    println(fast)
  }

  def sayHello(name:String)(whoAreYou: () => String) = {
    s"Hello $name! My name is ${whoAreYou()}"
  }

  def provideName() = {"Scala"}
}

println()
HelloWorld.main(Array())
println()