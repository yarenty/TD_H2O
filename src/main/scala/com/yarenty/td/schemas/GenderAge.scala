package com.yarenty.td.schemas

import water.parser._

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class GenderAge(val device_id: Option[String],
                val gender: Option[Int],
                val age: Option[Int],
                val group: Option[String]) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GenderAge]

  override def productArity: Int = 4

  override def productElement(n: Int) = n match {
    case 0 => device_id
    case 1 => gender
    case 2 => age
    case 3 => group
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
object GenderAgeParse extends Serializable {
  def apply(row: Array[String]): GenderAge = {

    import water.support.ParseSupport._

    new GenderAge(
      str(row(0)), // device_id
      int(row(1)), // gender
      int(row(2)), // age
      str(row(3)) // group
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/gender_age_train.csv"]
//  destination_frame: "gender_age_train.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 4
//  single_quotes: false
//  column_names: ["device_id","gender","age","group"]
//  column_types: ["String","Enum","Numeric","Enum"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 73953

object GenderAgeCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "device_id","gender","age","group")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "string", "enum", "enum", "string"))
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

object GenderAgeTestCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "device_id")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "string"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(1)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}
