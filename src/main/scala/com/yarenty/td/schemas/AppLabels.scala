package com.yarenty.td.schemas

import water.parser._

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class AppLabels(val app_id: Option[Long],
                val label_id: Option[Int]
            ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GenderAge]

  override def productArity: Int = 2

  override def productElement(n: Int) = n match {
    case 0 => app_id
    case 1 => label_id
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
object AppLabelsParse extends Serializable {
  def apply(row: Array[String]): AppLabels = {

    import water.support.ParseSupport._

    new AppLabels(
      long(row(0)), // labelid
      int(row(1)) // category
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/app_labels.csv"]
//  destination_frame: "app_labels.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 2
//  single_quotes: false
//  column_names: ["app_id","label_id"]
//  column_types: ["String","Numeric"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 349688

object AppLabelsCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "app_id","label_id")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "int", "enum"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(2)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}
