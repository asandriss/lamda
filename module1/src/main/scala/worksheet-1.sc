abstract class Person(fname: String, lname: String){
  def fullName = { s"$fname-$lname"}
}

case class Student(fname:String, lname:String, id:Int) extends Person(fname,lname)

val me = Student("Fedor", "Hajdu", 99)

def getFullId[T <: Person](something: T) = {
  something match {
    case Student(fname,lname,id) => s"$fname-$lname-$id"
    case p: Person => p.fullName
  }
}

getFullId(me)