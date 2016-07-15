package com.yarenty.td.schemas

import water.parser._

/**
  *
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class Event(val event_id: Option[Int],
                val device_id: Option[String],
                val timestamp: Long,
                val longitude: Option[Float],
                val latitude: Option[Float]
           ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Event]

  override def productArity: Int = 5

  override def productElement(n: Int) = n match {
    case 0 => event_id
    case 1 => device_id
    case 2 => timestamp
    case 3 => longitude
    case 4 => latitude
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def toString: String = {
    val sb = new StringBuffer
    for (i <- 0 until productArity)
      sb.append(productElement(i)).append(',')
    sb.toString
  }

  def isWrongRow(): Boolean = (0 until productArity)
    .map(idx => productElement(idx))
    .forall(e => e == None)
}

/** A dummy csv parser for orders dataset. */
object EventParse extends Serializable {
  def apply(row: Array[String]): Event = {

    import water.support.ParseSupport._

    new Event(
      int(row(0)), // device_id
      str(row(1)), // gender
      ParseTime.attemptTimeParse(new BufferedString(str(row(2)).get)), // age
      float(row(3)), // group
      float(row(4)) // group
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/events.csv"]
//  destination_frame: "events.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 5
//  single_quotes: false
//  column_names: ["event_id","device_id","timestamp","longitude","latitude"]
//  column_types: ["Numeric","String","Time","Numeric","Numeric"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 6107648

object EventCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "event_id","device_id","timestamp","longitude","latitude")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "int", "string", "time", "double", "double"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(5)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}
