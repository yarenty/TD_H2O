package com.yarenty.td.schemas

import water.parser._

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class PhoneBrand(val device_id: Option[String],
                val phone_brand: Option[String],
                val device_model: Option[String]
            ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GenderAge]

  override def productArity: Int = 3

  override def productElement(n: Int) = n match {
    case 0 => device_id
    case 1 => phone_brand
    case 2 => device_model
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
object PhoneBrandParse extends Serializable {
  def apply(row: Array[String]): PhoneBrand = {

    import water.support.ParseSupport._

    new PhoneBrand(
      str(row(0)), // device_id
      str(row(1)), // brand
      str(row(2)) // model
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/phone_brand_device_model.csv"]
//  destination_frame: "phone_brand_device_model.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 3
//  single_quotes: false
//  column_names: ["device_id","phone_brand","device_model"]
//  column_types: ["String","Enum","Enum"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 209864

object PhoneBrandCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "device_id","phone_brand","device_model")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "int", "enum", "enum"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(3)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}
