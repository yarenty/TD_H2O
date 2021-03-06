package com.yarenty.td.schemas

import water.parser._

/**
  *
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class AppEvent(val event_id: Option[Int],
                val app_id: Option[Long],
                val is_installed: Option[Int],
                val is_active: Option[Int]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[AppEvent]

  override def productArity: Int = 4

  override def productElement(n: Int) = n match {
    case 0 => event_id
    case 1 => app_id
    case 2 => is_installed
    case 3 => is_active
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
object AppEventParse extends Serializable {
  def apply(row: Array[String]): AppEvent = {

    import water.support.ParseSupport._

    new AppEvent(
      int(row(0)), // device_id
      long(row(1)), // gender
      int(row(2)), // age
      int(row(3)) // group
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/app_events.csv"]
//  destination_frame: "app_events.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 4
//  single_quotes: false
//  column_names: ["event_id","app_id","is_installed","is_active"]
//  column_types: ["Numeric","String","Numeric","Numeric"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 32414720

object AppEventCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "event_id","app_id","is_installed","is_active")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "int", "int", "int", "int"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(4)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}
