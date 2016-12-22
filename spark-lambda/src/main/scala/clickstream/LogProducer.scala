package clickstream

import java.io.FileWriter

import config.Settings

import scala.util.Random

/**
  * Created by Fedor.Hajdu on 12/22/2016.
  */
object LogProducer extends App {
  val wlc = Settings.WebLogGen

  // log sample data from resources
  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray

  // generate visitors and pages based on range allowed
  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  val Pages = (0 to wlc.pages).map("Page-" + _)

  val rnd = new Random()

  val filePath = wlc.filePath
  val fw = new FileWriter(filePath, true)
  val incrementTimeEvery = rnd.nextInt(wlc.records - 1) + 1

  var timestamp = System.currentTimeMillis()
  var adjustedTimestamp = timestamp

  for (iteration <- 1 to wlc.records){
    adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) + wlc.timeMultiplier)
    timestamp = System.currentTimeMillis()

    val action = iteration % (rnd.nextInt(200) + 1) match {
      case 0 => "purchase"
      case 1 => "add_to_cart"
      case _ => "page_view"
    }

    val referrer = Referrers(rnd.nextInt(Referrers.length-1))
    val prevPage = referrer match {
      case "Internal" => Pages(rnd.nextInt(Pages.length-1))
      case _ => ""
    }
  }
}
