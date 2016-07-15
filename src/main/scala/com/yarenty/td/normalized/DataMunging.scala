package com.yarenty.td.normalized

import java.io.{File, PrintWriter}

import com.yarenty.td.utils.Helper
import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import scala.collection.mutable
import scala.collection.mutable.HashMap

//import org.apache.spark.{SparkFiles, h2o, SparkContext}
//import org.apache.spark.h2o.{RDD, H2OFrame, DoubleHolder, H2OContext}
import com.yarenty.td.schemas._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import water.support.SparkContextSupport


/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object DataMunging extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val gender_age_train = "gender_age_train.csv"
  val gender_age_test = "gender_age_test.csv"
  val app_events = "app_events.csv"
  val events = "events.csv"
  val app_labels = "app_labels.csv"
  val phone_brand = "phone_brand_device_model.csv"



  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + gender_age_train),
      absPath(input_data_dir + gender_age_test),
      absPath(input_data_dir + phone_brand),
      absPath(input_data_dir + app_events),
      absPath(input_data_dir + app_labels),
      absPath(input_data_dir + events)
    )


    val genderAgeData =  new h2o.H2OFrame(GenderAgeCSVParser.get, new File(SparkFiles.get(gender_age_train)))
    println(s"\n===> genderAge via H2O#Frame#count: ${genderAgeData.numRows}\n")
    val genderAgeTable = asRDD[GenderAge](genderAgeData)


    val appEventData =  new h2o.H2OFrame(AppEventCSVParser.get, new File(SparkFiles.get(app_events)))
    println(s"\n===> appEventData via H2O#Frame#count: ${ appEventData.numRows}\n")
    val appEventTable = asRDD[AppEvent](appEventData)


    val phoneBrandData =  new h2o.H2OFrame(PhoneBrandCSVParser.get, new File(SparkFiles.get(phone_brand)))
    println(s"\n===> phoneBrandData via H2O#Frame#count: ${phoneBrandData.numRows}\n")
    val phoneBrandTable = asRDD[PhoneBrand](phoneBrandData)






//      val myData = new h2o.H2OFrame(lineBuilder(headers, types,
//        genderAgeTable
//))
//
//      val v = DKV.put(myData)

//      println(s" AND MY DATA IS: ${myData.key} =>  ${myData.numCols()} / ${myData.numRows()}")
//      println(s" frame::${v}")

//
//    Helper.saveCSV(myData, data_dir + "train")
//
//      println(
//        s"""
//           |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//           |!!  OUTPUT CREATED: ${data_dir}train !!
//           |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//         """.stripMargin)

      //clean
//      myData.delete()


      println("... and cleaned")

  }


  def lineBuilder(headers: Array[String], types: Array[Byte],
                  dayOfWeek: Int,
                  gaps: Map[Int, Int],
                  traffic: Map[Int, Tuple4[Double, Double, Double, Double]],
                  weather: Map[Int, Tuple3[Int, Double, Double]],
                  poi: Map[Int, Map[String, Double]]): Frame = {

    val len = headers.length

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_NUM)
      chunks(i) = new NewChunk(av(i), 0)
    }

    for (ts <- 1 to 144) {
      for (din <- 1 to 66) {
        for (dout <- 0 to 66) {
          val idx = ts
          chunks(0).addNum(idx)
          chunks(1).addNum(ts)
          chunks(2).addNum(din)
          if (dout == 0) {
            //chunks(3).addNA()  - is not really unknown as its just outside city
            chunks(3).addNum(0)
          } else {
            chunks(3).addNum(dout)
          }

          chunks(4).addNum(dayOfWeek)


          if (gaps.contains(idx)) {
            chunks(5).addNum(gaps.get(idx).get)
          }
          else {
            chunks(5).addNum(0)
          }

          var tidx = ts * 100 + din
          if (traffic.contains(tidx)) {
            chunks(6).addNum(traffic.get(tidx).get._1)
            chunks(7).addNum(traffic.get(tidx).get._2)
            chunks(8).addNum(traffic.get(tidx).get._3)
            chunks(9).addNum(traffic.get(tidx).get._4)
          }
          else {
            chunks(6).addNA()
            chunks(7).addNA()
            chunks(8).addNA()
            chunks(9).addNA()
          }

          if (weather.contains(ts)) {
            chunks(10).addNum(weather.get(ts).get._1)
            chunks(11).addNum(weather.get(ts).get._2.toDouble)
            chunks(12).addNum(weather.get(ts).get._3.toDouble)
          } else {
            chunks(10).addNA()
            chunks(11).addNA()
            chunks(12).addNA()
          }

          for (pp <- 13 until len) {

            if (poi.contains(din)) {
              val m = poi.get(din).get

              if (m.contains(headers(pp))) {
                chunks(pp).addNum(m.get(headers(pp)).get)
              }
              else {
                chunks(pp).addNum(0)
              }
            }
            else {
              chunks(pp).addNA()

            }
          }


        }
      }
    }

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make("Events")
    return new Frame(key, headers, vecs)

  }



}
