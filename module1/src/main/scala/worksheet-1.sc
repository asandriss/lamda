object HelloWorld {
  def main(args: Array[String]): Unit = {
    val text : String = "Hello world"
    println(text)
  }

  def sayHello(name: String) : String = {
    s"Hello $name"    // s is used for string interpolation
  }
}

HelloWorld.main(Array())
