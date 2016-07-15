package com.yarenty.td.normalized

import java.io.{File, PrintWriter}

import com.yarenty.td.utils.Helper
import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import scala.collection
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
  val label_categories = "label_categories.csv"
  val phone_brand = "phone_brand_device_model.csv"

  val output_filename = "/opt/data/TalkingData/model/train"


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
      absPath(input_data_dir + label_categories),
      absPath(input_data_dir + events)
    )


    val eventData = new h2o.H2OFrame(EventCSVParser.get, new File(SparkFiles.get(events)))
    println(s"\n===> eventData via H2O#Frame#count: ${eventData.numRows}\n")
    val eventTable = asRDD[EventIN](eventData)
      .map(row => EventParse(row))
      .filter(!_.isWrongRow())

    val eventMap = eventTable.map(e => {
      e.event_id ->(e.device_id, e.timestamp, e.longitude, e.latitude)
    }).collectAsMap

//    eventData.delete()
//    eventTable.delete()


    val genderAgeData = new h2o.H2OFrame(GenderAgeCSVParser.get, new File(SparkFiles.get(gender_age_train)))
    println(s"\n===> genderAge via H2O#Frame#count: ${genderAgeData.numRows}\n")
    val genderAgeMap = asRDD[GenderAge](genderAgeData).map(g => {
      g.device_id ->(g.age, g.gender)
    }).collectAsMap

//    genderAgeData.delete()



    val phoneBrandData = new h2o.H2OFrame(PhoneBrandCSVParser.get, new File(SparkFiles.get(phone_brand)))
    println(s"\n===> phoneBrandData via H2O#Frame#count: ${phoneBrandData.numRows}\n")
    val phoneBrandMap = asRDD[PhoneBrand](phoneBrandData).map(p => {
      p.device_id ->(p.phone_brand, p.device_model)
    }).collectAsMap

//    phoneBrandData.delete()

    val appLabelsData = new h2o.H2OFrame(AppLabelsCSVParser.get, new File(SparkFiles.get(app_labels)))
    println(s"\n===> appLabelsData via H2O#Frame#count: ${appLabelsData.numRows}\n")
    val appLabelsMap = asRDD[AppLabels](appLabelsData).map(l => {
      l.app_id -> l.label_id
    }).collectAsMap

//    appLabelsData.delete()

    //    val labelCatData =  new h2o.H2OFrame(LabelCategoriesCSVParser.get, new File(SparkFiles.get(label_categories)))
    //    println(s"\n===> labelCatData via H2O#Frame#count: ${labelCatData.numRows}\n")
    //    val labelCatTable = asRDD[LabelCategories](labelCatData)


    val appEventData = new h2o.H2OFrame(AppEventCSVParser.get, new File(SparkFiles.get(app_events)))
    println(s"\n===> appEventData via H2O#Frame#count: ${appEventData.numRows}\n")
    val appEventTable = asRDD[AppEvent](appEventData)




    val myData = new h2o.H2OFrame(lineBuilder(
      appEventTable,
      phoneBrandMap,
      genderAgeMap,
      eventMap,
      appLabelsMap
    ))

    val v = DKV.put(myData)

    println(s" AND MY DATA IS: ${myData.key} =>  ${myData.numCols()} / ${myData.numRows()}")
    println(s" frame::${v}")


    Helper.saveCSV(myData, output_filename)

    println(
      s"""
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         |!!  OUTPUT CREATED: ${output_filename} !!
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         """.stripMargin)

    //clean
    //      myData.delete()


    println("... and cleaned")

  }


  def lineBuilder(
                   aeMap: RDD[AppEvent],
                   phoneBrandMap: collection.Map[Option[Long], (Option[String], Option[String])],
                   genderAgeMap: collection.Map[Option[Long], (Option[Int], Option[Int])],
                   eventMap: collection.Map[Option[Int], (Option[Long], Option[Int], Option[Float], Option[Float])],
                   appLabelsMap: collection.Map[Option[Long], Option[Int]]
                 ): Frame = {

    val headers = Array(
      "gender", "age", "timeslice", "lat", "lon",
      "active", "label",
      "brand", "model")

    val types = Array(Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM, Vec.T_NUM,
      Vec.T_NUM, Vec.T_NUM,
      Vec.T_STR, Vec.T_STR)


    val len = Helper.output_headers.length

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Helper.output_types(i))
      chunks(i) = new NewChunk(av(i), 0)
    }


    aeMap.map(ae => {

      if (eventMap.contains(ae.event_id)) {

        val event = eventMap.get(ae.event_id).get
        val devID = event._1


        if (genderAgeMap.contains(devID)) {
          val gen = genderAgeMap.get(devID).get
         // val phone = phoneBrandMap.get(devID).get
          val label = appLabelsMap.get(ae.app_id).get


          chunks(0).addNum(gen._2.get) //gender
          chunks(1).addNum(gen._1.get) //age

          chunks(2).addNum(event._2.get)
          chunks(3).addNum(event._3.get)
          chunks(4).addNum(event._4.get)

          chunks(5).addNum(ae.is_active.get) //active

          chunks(6).addNum(label.get) //label id -- type

          //chunks(7).addStr(phone._1.get) //brand
          //chunks(8).addStr(phone._2.get) //brand
        }
      }
    }).count()

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make("Events")
    return new Frame(key, headers, vecs)

  }


}
