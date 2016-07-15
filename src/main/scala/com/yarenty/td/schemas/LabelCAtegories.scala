package com.yarenty.td.schemas

import water.parser._

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
class LabelCategories(val label_id: Option[Int],
                val category: Option[String]
            ) extends Product with Serializable {

  override def canEqual(that: Any): Boolean = that.isInstanceOf[GenderAge]

  override def productArity: Int = 2

  override def productElement(n: Int) = n match {
    case 0 => label_id
    case 1 => category
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
object LabelCategoriesParse extends Serializable {
  def apply(row: Array[String]): LabelCategories = {

    import water.support.ParseSupport._

    new LabelCategories(
      int(row(0)), // labelid
      str(row(1)) // category
    )
  }
}


//parseFiles
//  paths: ["/opt/data/TalkingData/input/label_categories.csv"]
//  destination_frame: "label_categories.hex"
//  parse_type: "CSV"
//  separator: 44
//  number_columns: 2
//  single_quotes: false
//  column_names: ["label_id","category"]
//  column_types: ["Numeric","Enum"]
//  delete_on_done: true
//  check_header: 1
//  chunk_size: 4194304

object LabelCategoriesCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "label_id","category")
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
