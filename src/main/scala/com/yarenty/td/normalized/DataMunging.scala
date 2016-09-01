package com.yarenty.td.normalized

import java.io.File

import com.yarenty.td.utils.Helper
import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import com.yarenty.td.schemas._
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.support.SparkContextSupport


/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object DataMunging extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val gender_age_train = "gender_age_train.csv"
  val app_events = "app_events.csv"
  val events = "events.csv"
  val timeevents = "newevents_train.csv"
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
      absPath(input_data_dir + phone_brand),
      absPath(input_data_dir + app_events),
      absPath(input_data_dir + app_labels),
      absPath(input_data_dir + label_categories),
      absPath(input_data_dir + events),
      absPath(input_data_dir + timeevents)
    )



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



    val genderAgeData = new h2o.H2OFrame(GenderAgeCSVParser.get, new File(SparkFiles.get(gender_age_train)))
    println(s"\n===> genderAge via H2O#Frame#count: ${genderAgeData.numRows}\n")
    val genderAgeDF = asDataFrame(genderAgeData)
    genderAgeDF.registerTempTable("genderage")


    val eventData = new h2o.H2OFrame(EventCSVParser.get, new File(SparkFiles.get(events)))
    println(s"\n===> eventData via H2O#Frame#count: ${eventData.numRows}\n")
    val eventDF = asDataFrame(eventData)
    eventDF.registerTempTable("events")

    val timeEventData = new h2o.H2OFrame(EventsByTimeCSVParser.get, new File(SparkFiles.get(timeevents)))
    println(s"\n===> timeEventData via H2O#Frame#count: ${timeEventData.numRows}\n")
    val timeEventDF = asDataFrame(eventData)
    timeEventDF.registerTempTable("timeevents")


    val appEventData = new h2o.H2OFrame(AppEventCSVParser.get, new File(SparkFiles.get(app_events)))
    println(s"\n===> appEventData via H2O#Frame#count: ${appEventData.numRows}\n")
    val appEvenDF = asDataFrame(appEventData)
    appEvenDF.registerTempTable("apps")

    val appLabelsData = new h2o.H2OFrame(AppLabelsCSVParser.get, new File(SparkFiles.get(app_labels)))
    println(s"\n===> appLabelsData via H2O#Frame#count: ${appLabelsData.numRows}\n")
    val appsDF = asDataFrame(appLabelsData)
    appsDF.registerTempTable("labels")

    val phoneBrandData = new h2o.H2OFrame(PhoneBrandCSVParser.get, new File(SparkFiles.get(phone_brand)))
    println(s"\n===> phoneBrandData via H2O#Frame#count: ${phoneBrandData.numRows}\n")
    val phoneBrandDF = asDataFrame(phoneBrandData)
    phoneBrandDF.registerTempTable("phones")



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



    ///get list of apptypes for each device_id
    val aps = sqlContext.sql(" select distinct device_id, label_id, apps.app_id as app_id from labels, apps, events " +
      " where labels.app_id = apps.app_id and apps.event_id = events.event_id ")
    aps.registerTempTable("apps")


    val ap = sqlContext.sql(" select device_id, label_id, count(app_id) as ile from apps " +
      " group by device_id, label_id ")
    ap.registerTempTable("apptypes")


    val o = sqlContext.sql("select g.device_id as device_id, gender, age, phone_brand, device_model, grup, label_id, ile " +
      " from " +
      "  (select genderage.device_id as device_id, " +
      "     first(gender) as gender, first(age) as age, first(group) as grup, " +
      "     first(phone_brand) as phone_brand, first(device_model) as device_model " +
      "     from genderage , phones  where genderage.device_id = phones.device_id group by genderage.device_id) as g" +
      "  LEFT JOIN  apptypes ON g.device_id = apptypes.device_id ")

    val out = o.groupBy("device_id", "gender", "age", "phone_brand", "device_model", "grup").agg(
      GroupConcat(o("label_id")).alias("labels"),GroupConcat(o("ile")).alias("iles")).distinct()

    out.take(20).foreach(println)

    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



    val myData = new h2o.H2OFrame(lineBuilder(
      out
    ))

    println(s" AND MY DATA IS: ${myData.key} =>  ${myData.numCols()} / ${myData.numRows()}")

    Helper.saveCSV(myData, output_filename)

    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



    println(
      s"""
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         |!!  OUTPUT CREATED: ${output_filename} !!
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         """.stripMargin)

    //clean
    genderAgeData.delete()
    eventData.delete()
    appEventData.delete()
    appLabelsData.delete()
    phoneBrandData.delete()
    println("... and cleaned")

  }


  //@TODO: Dropping constant columns:


  def lineBuilder(out: DataFrame): Frame = {

    val headers = Array(
      "device", "gender", "age", "brand", "model", "grup") ++ (1 to 1021).map(i => i.toString) ++ (1 to 144).map(i => "t"+i.toString)
    val startIdx = 6
    val len = headers.length
    println("LEN::" + len)

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      if (i==0 || i == 3 || i == 4 || i == 5)
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_STR)
      else
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_NUM)
      chunks(i) = new NewChunk(av(i), 0)
    }

    println("Structure is there")
    out.collect.foreach(ae => {     //collect

      //println("AND::" + ae.get(0)+ "::"+ae.get(1) +"::"+ae.get(2))
      chunks(0).addStr(ae.getString(0)) //device_id
      val gender: Int = if (ae.getString(1).equals("F")) 0 else 1
      chunks(1).addNum(gender)
      chunks(2).addNum(ae.getString(2).toInt) //age
      chunks(3).addStr(ae.getString(3))
      chunks(4).addStr(ae.getString(4))
      chunks(5).addStr(ae.getString(5))

      if (ae.get(startIdx) != null && ae.getString(startIdx).length > 2) {
        val m = Helper.mapCreator(ae.getString(startIdx),ae.getString(startIdx+1))
        for (i <- startIdx until len) chunks(i).addNum(m(i - startIdx))
      } else {
        for (i <- startIdx until len) chunks(i).addNA()
      }

    })

    println("Finalize")

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make("Apps")
    return new Frame(key, headers, vecs)

  }




 // buildModel 'kmeans',
  // {"model_id":"kmeans-33542bca-a6e7-4360-af37-812c5629a315",
  // "training_frame":"events.hex","nfolds":0,
  // "ignored_columns":["event_id","device_id"],
  // "ignore_const_cols":true,
  // "k":"100",
  // "max_iterations":1000,"init":"Furthest",
  // "score_each_iteration":false,
  // "standardize":false, !!!!
  // "max_runtime_secs":0,"seed":101570672145523}


}
