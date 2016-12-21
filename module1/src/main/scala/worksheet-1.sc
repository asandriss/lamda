abstract class Person(fname: String, lname: String){
  def fullName = { s"$fname-$lname"}
}

case class Student(fname:String, lname:String, id:Int) extends Person(fname,lname)

val me = Student("Fedor", "Hajdu", 99)

def getFullId[T <: Person](something: T) = {      // c# equivalent getFullId<T>(T var) where T: Person
  something match {
    case Student(fname,lname,id) => s"$fname-$lname-$id"
    case p: Person => p.fullName
  }
}

getFullId(me)

implicit class stringUtils(myString: String){
  def scalaWordCount() =  {
    val split = myString.split("\\s+")
    val grouped = split.groupBy(word => word)
    val countPerKey = grouped.mapValues(group => group.length)
    countPerKey
  }
}

"Spark collections mimic Scala collections".scalaWordCount()