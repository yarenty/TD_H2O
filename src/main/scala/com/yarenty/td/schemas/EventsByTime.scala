package com.yarenty.td.schemas

import water.parser.{DefaultParserProviders, ParseSetup}

/**
  *
  * parseFiles
  * paths: ["/opt/data/TalkingData/input/newevents_train.csv"]
  * destination_frame: "newevents_train.hex"
  * parse_type: "CSV"
  * separator: 44
  * number_columns: 145
  * single_quotes: false
  * column_names: ["device","t1","t2","t3","t4","t5","t6","t7","t8","t9","t10","t11","t12","t13","t14","t15","t16","t17","t18","t19","t20","t21","t22","t23","t24","t25","t26","t27","t28","t29","t30","t31","t32","t33","t34","t35","t36","t37","t38","t39","t40","t41","t42","t43","t44","t45","t46","t47","t48","t49","t50","t51","t52","t53","t54","t55","t56","t57","t58","t59","t60","t61","t62","t63","t64","t65","t66","t67","t68","t69","t70","t71","t72","t73","t74","t75","t76","t77","t78","t79","t80","t81","t82","t83","t84","t85","t86","t87","t88","t89","t90","t91","t92","t93","t94","t95","t96","t97","t98","t99","t100","t101","t102","t103","t104","t105","t106","t107","t108","t109","t110","t111","t112","t113","t114","t115","t116","t117","t118","t119","t120","t121","t122","t123","t124","t125","t126","t127","t128","t129","t130","t131","t132","t133","t134","t135","t136","t137","t138","t139","t140","t141","t142","t143","t144"]
  * column_types: ["int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int"]
  * delete_on_done: true
  * check_header: 1
  * chunk_size: 592043
  *
  * Created by yarenty on 27/08/2016.
  * (C)2015 SkyCorp Ltd.
  */
object EventsByTimeCSVParser {

  def get: ParseSetup = {
    val parseOrders: ParseSetup = new ParseSetup()
    val orderNames: Array[String] = Array(
      "device","t1","t2","t3","t4","t5","t6","t7","t8","t9","t10","t11","t12","t13","t14","t15","t16","t17","t18","t19","t20","t21","t22","t23","t24","t25","t26","t27","t28","t29","t30","t31","t32","t33","t34","t35","t36","t37","t38","t39","t40","t41","t42","t43","t44","t45","t46","t47","t48","t49","t50","t51","t52","t53","t54","t55","t56","t57","t58","t59","t60","t61","t62","t63","t64","t65","t66","t67","t68","t69","t70","t71","t72","t73","t74","t75","t76","t77","t78","t79","t80","t81","t82","t83","t84","t85","t86","t87","t88","t89","t90","t91","t92","t93","t94","t95","t96","t97","t98","t99","t100","t101","t102","t103","t104","t105","t106","t107","t108","t109","t110","t111","t112","t113","t114","t115","t116","t117","t118","t119","t120","t121","t122","t123","t124","t125","t126","t127","t128","t129","t130","t131","t132","t133","t134","t135","t136","t137","t138","t139","t140","t141","t142","t143","t144")
    val orderTypes = ParseSetup.strToColumnTypes(Array(
      "numeric","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int","int"))
    parseOrders.setColumnNames(orderNames)
    parseOrders.setColumnTypes(orderTypes)
    parseOrders.setParseType(DefaultParserProviders.CSV_INFO)
    parseOrders.setNumberColumns(145)
    parseOrders.setSeparator(44)
    parseOrders.setSingleQuotes(false)
    parseOrders.setCheckHeader(1)
    return parseOrders
  }

}