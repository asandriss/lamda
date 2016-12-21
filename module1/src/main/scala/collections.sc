val myList = List("Spark", "mimics", "Scala", "collections")

val mapped = myList.map(s => s.toUpperCase)

val flatMapped = myList.flatMap { s =>
  var filters = List("mimics", "collections")
  if(filters.contains(s))
    None
  else
    Some(s)
}