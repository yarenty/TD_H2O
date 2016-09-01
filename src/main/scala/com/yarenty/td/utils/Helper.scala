package com.yarenty.td.utils

import java.io.{File, PrintWriter, FileOutputStream}

import hex.tree.drf.DRFModel
import water.AutoBuffer
import water.fvec.{Frame, Vec}

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object Helper {


  val names = Array("device_id","F23-","F24-26","F27-28","F29-32","F33-42","F43+","M22-","M23-26","M27-28","M29-31","M32-38","M39+")

  val enumColumns = Array("device_id")

  val output_headers = Array("device_id","gender","age","group")

  val output_types = Array(Vec.T_NUM, Vec.T_CAT, Vec.T_CAT, Vec.T_CAT, Vec.T_NUM, Vec.T_NUM,
    Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM,
    Vec.T_CAT, Vec.T_NUM, Vec.T_NUM,
    Vec.T_NUM)



  def saveJavaPOJOModel(model:DRFModel, path:String): Unit ={
    val name = path + System.currentTimeMillis() + ".java"
    val om = new FileOutputStream(name)
    model.toJava(om, false, false)
    println("Java POJO model saved as"+name)
  }

  def saveBinaryModel(model:DRFModel, path:String): Unit ={
    val name = path + System.currentTimeMillis() + ".hex"
    val omab = new FileOutputStream(name)
    val ab = new AutoBuffer(omab, true)
    model.write(ab)
    ab.close()
    println("HEX(iced) model saved as"+name)
  }


  def saveCSV(f:Frame, name: String): Unit ={
    val csv = f.toCSV(true, false)
    val csv_writer = new PrintWriter(new File(name))
    while (csv.available() > 0) {
      csv_writer.write(csv.read.toChar)
    }
    csv_writer.close
  }


  /**
    * Create array (1021) of int values where = 1 if index was in input string
    * @param str
    * @return
    */
  def mapCreator(str: String, vals:String): Array[Int] = {
    val all = str.split(",").map( i => i.toInt)
    val valall = vals.split(",").map( i => i.toInt)
    val out = new Array[Int](1021)
    for (i <- 0 to 1020) {
      if (all.contains(i+1)) {
        out(i) = valall(all.indexOf(i+1))
      } else
      out(i) = 0
    }
    out
  }


  def main(args: Array[String]) {

    val in = Array("id", "timeslice", "district ID", "destDistrict", "demand", "gap",
      "traffic1", "traffic2", "traffic3", "traffic4", "weather", "temp", "pollution",
      "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p10",
      "p11", "p12", "p13", "p14", "p15", "p16", "p17", "p18", "p19", "p20",
      "p21", "p22", "p23", "p24", "p25")

    //    for (i <- in) println ("val "+i+": Option[Int],")
    //    var x=0
    //    for (i <- in ) {
    //      println("case "+x+" => " + i)
    //      x += 1
    //    }

    var x = 0
    for (i <- in) {
      println("int(row(" + x + ")), //" + i)
      x += 1
    }

  }

}
