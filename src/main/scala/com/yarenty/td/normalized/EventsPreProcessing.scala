package com.yarenty.td.normalized


import java.io.File
import java.util

import com.yarenty.td.schemas.AppEventCSVParser
import com.yarenty.td.schemas.AppLabelsCSVParser
import com.yarenty.td.schemas.EventCSVParser
import com.yarenty.td.schemas.GenderAgeCSVParser
import com.yarenty.td.schemas.PhoneBrandCSVParser
import com.yarenty.td.utils.Helper
import com.yarenty.td.utils.Helper
import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import com.yarenty.td.schemas._
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.support.SparkContextSupport

import scala.collection.mutable.Map


/**
  * Created by yarenty on 24/08/2016.
  * (C)2015 SkyCorp Ltd.
  */
object EventsPreProcessing extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val events = "events.csv"
  val gender_age_train = "gender_age_train.csv"
  val output_filename = "/opt/data/TalkingData/input/newevents_train"


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + gender_age_train),
      absPath(input_data_dir + events)
    )



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)


//    val genderAgeData = new h2o.H2OFrame(GenderAgeCSVParser.get, new File(SparkFiles.get(gender_age_train)))
//    println(s"\n===> genderAge via H2O#Frame#count: ${genderAgeData.numRows}\n")
//    val genderAgeDF = asDataFrame(genderAgeData)
//    genderAgeDF.registerTempTable("genderage")


    val eventData = new h2o.H2OFrame(EventCSVParser.get, new File(SparkFiles.get(events)))
    println(s"\n===> eventData via H2O#Frame#count: ${eventData.numRows}\n")

    val eventTable: h2o.RDD[Event] = asRDD[EventIN](eventData).map(row => EventParse(row)).filter(!_.isWrongRow())
    // now I have events with timeslices instead timestamp

    val ev = new h2o.H2OFrame(eventTable)




    val eventDF = asDataFrame(ev)
    eventDF.registerTempTable("events")


    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)

    var x = 3
    val tpattern = sqlContext.sql(" select device_id, timeslice, count(*) as c from events " +
      " where 1=1 group by device_id, timeslice order by device_id ")
    tpattern.registerTempTable("tpattern")
    println("LENGTH:: " + tpattern.collect().length)

    val oMap: Map[Long, Map[Int, Int]] = Map[Long, Map[Int, Int]]()

    val z:scala.collection.Map[Long, Map[Int, Int]] = tpattern.flatMap(row => {

      val did: Long = row.getAs[String]("device_id").toLong
      val ts = row.getAs[Short]("timeslice").toInt
      val c = row.getAs[Long]("c").toInt

      if (x >0 ) {
//        println("DO:" + did + "," + ts + "," + c)
        x = x-1
      }

      if (oMap.contains(did)) {
        oMap.get(did).get += (ts -> c)
//        if (x >0 ) println ("old:"+oMap)
      } else {
        val nm = Map(ts -> c)
        oMap += (did -> nm)
//        if (x >0 ) println ("new:"+ oMap)
      }

      oMap
    }).collectAsMap

    println("MAP size" + oMap.size)
    println("ZZZ MAP size" + z.size)

    val out = sqlContext.sql("select distinct device_id from tpattern")

    println(" [TPATTERN] number of devices == " + out.count  )
    println(" [TPATTERN] while processed == " + tpattern.count  )
    tpattern.take(20).foreach(println)


    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)

    val myData = new h2o.H2OFrame(lineBuilder(
       z
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
    eventData.delete()
    println("... and cleaned")

  }


  //@TODO: Dropping constant columns:


  def lineBuilder( oMap:scala.collection.Map[Long, Map[Int, Int]]): Frame = {

    val headers = Array(
      "device") ++ (1 to 144).map(i => "t" + i.toString)
    val len = headers.length
    println("LEN::" + len)

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      if (i == 0)
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_STR)
      else
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_NUM)
      chunks(i) = new NewChunk(av(i), 0)
    }

    println("Structure is there, map:" + oMap.size + " empty:" + oMap.isEmpty)
    oMap.foreach(ae => {
      //collect
        chunks(0).addStr(ae._1.toString)

        val dM: Map[Int, Int] = ae._2
        for (i <- 1 to 144) {
          if (dM.contains(i)) {
            chunks(i).addNum(
              dM.get(i).get.toDouble)
          } else {
            chunks(i).addNum(0)
          }
        }

    })

    println("Finalize")

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make("Events")
    return new Frame(key, headers, vecs)

  }



}
