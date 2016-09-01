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
object DataMungingTest extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val gender_age_test = "gender_age_test.csv"
  val app_events = "app_events.csv"
  val events = "events.csv"
  val app_labels = "app_labels.csv"
  val label_categories = "label_categories.csv"
  val phone_brand = "phone_brand_device_model.csv"

  val output_filename = "/opt/data/TalkingData/model/test"


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + gender_age_test),
      absPath(input_data_dir + phone_brand),
      absPath(input_data_dir + app_events),
      absPath(input_data_dir + app_labels),
      absPath(input_data_dir + label_categories),
      absPath(input_data_dir + events)
    )



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)


    val genderAgeData = new h2o.H2OFrame(GenderAgeTestCSVParser.get, new File(SparkFiles.get(gender_age_test)))
    println(s"\n===> genderAge via H2O#Frame#count: ${genderAgeData.numRows}\n")
    val genderAgeDF = asDataFrame(genderAgeData)
    genderAgeDF.registerTempTable("genderage")


    val eventData = new h2o.H2OFrame(EventCSVParser.get, new File(SparkFiles.get(events)))
    println(s"\n===> eventData via H2O#Frame#count: ${eventData.numRows}\n")
    val eventDF = asDataFrame(eventData)
    eventDF.registerTempTable("events")


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

    val aps = sqlContext.sql(" select distinct device_id, label_id, apps.app_id as app_id from labels, apps, events " +
      " where labels.app_id = apps.app_id and apps.event_id = events.event_id ")
    aps.registerTempTable("apps")


    val ap = sqlContext.sql(" select device_id, label_id, count(app_id) as ile from apps " +
      " group by device_id, label_id ")
    ap.registerTempTable("apptypes")


    val o = sqlContext.sql("select g.device_id as device_id,  \"M\" as gender, \"0\" as age, \"G\" as grup, phone_brand, device_model, label_id, ile " +
      " from " +
      "  (select genderage.device_id as device_id, " +
      "     first(phone_brand) as phone_brand, first(device_model) as device_model " +
      "     from genderage , phones  where genderage.device_id = phones.device_id group by genderage.device_id) as g" +
      "  LEFT JOIN  apptypes ON g.device_id = apptypes.device_id ")

    val out = o.groupBy("device_id", "gender", "age", "phone_brand", "device_model", "grup").agg(
      GroupConcat(o("label_id")).alias("labels"),GroupConcat(o("ile")).alias("iles")).distinct()

    out.take(20).foreach(println)



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


  def lineBuilder(out: DataFrame): Frame = {

    val headers = Array(
      "device", "gender", "age", "brand", "model", "grup") ++ (1 to 1021).map(i => i.toString)
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


    out.collect.foreach(ae => {


//      ERROR: The value '-6590454305031525112' in the key column 'device_id' has already been defined (Line 28569, Column 1)
//      ERROR: The value '-7059081542575379359' in the key column 'device_id' has already been defined (Line 89083, Column 1)
//      ERROR: The value '-5269721363279128080' in the key column 'device_id' has already been defined (Line 97610, Column 1)
//      ERROR: The value '-7297178577997113203' in the key column 'device_id' has already been defined (Line 101556, Column 1)
//      ERROR: The value '-3004353610608679970' in the key column 'device_id' has already been defined (Line 102515, Column 1).
//
//      val ss =  Array("-6590454305031525112","-7059081542575379359","-7297178577997113344",
//        "1186608308763918336","-3004353610608679936","-7059081542575379456")
//      if (ss.contains(ae.getString(0))) {
//        println("AND::" + ae.getString(0)+ "::"+ae.getString(3) +"::"+ae.getString(4)+"  >> "+ae.getString(startIdx))
//      }
      chunks(0).addStr(ae.getString(0)) //device_id
      chunks(1).addNA()
      chunks(2).addNA()

      chunks(3).addStr(ae.getString(3))
      chunks(4).addStr(ae.getString(4))
      chunks(5).addNA()



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

    val key = Key.make("TestApps")
    return new Frame(key, headers, vecs)
  }


}
