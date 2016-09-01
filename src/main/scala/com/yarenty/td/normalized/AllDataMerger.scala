package com.yarenty.td.normalized

import java.io.File
import java.net.URI

import com.yarenty.td.schemas._
import com.yarenty.td.utils.Helper
import org.apache.spark._
import org.apache.spark.h2o._
import org.apache.spark.sql.{DataFrame, SQLContext}
import water._
import water.fvec._
import water.support.SparkContextSupport


/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object AllDataMerger extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/model/"
  val timeevents = "timeevents.csv"

  val test = "dev_app_train.csv"
//  val test = "dev_app_test.csv"

  val allev = "events_noEvents"
  val evNolat = "events_nolat"
  val clustfile = "events_clusters"

//  val output_full_filename = "/opt/data/TalkingData/model/all_test"
  val output_full_filename = "/opt/data/TalkingData/model/all_train"

  def process(h2oContext: H2OContext) {

    import h2oContext._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + test),
      absPath(input_data_dir + allev),
      absPath(input_data_dir + evNolat),
      absPath(input_data_dir + clustfile),
      absPath(input_data_dir + timeevents)
    )


    val tData = new h2o.H2OFrame(ModelCSVParser.get, new URI("file:///" + SparkFiles.get(test)))
    val tDF = asDataFrame(tData)
    tDF.registerTempTable("apps")

    val timeEventData = new h2o.H2OFrame(EventsByTimeCSVParser.get, new File(SparkFiles.get(timeevents)))
    println(s"\n===> timeEventData via H2O#Frame#count: ${timeEventData.numRows}\n")
    val timeEventDF = asDataFrame(timeEventData)
    timeEventDF.registerTempTable("timeevents")

    val ids = (1 to 143).map(i => "t" + i.toString).mkString(",")
    ///get list of apptypes for each device_id
    val aps = sqlContext.sql(" select apps.*, " + ids + " t144 from apps LEFT OUTER JOIN timeevents ON apps.device = timeevents.device")
    aps.registerTempTable("mm")


    val evAll = new h2o.H2OFrame( new URI("file:///" + SparkFiles.get(allev)))
     val eaDF = asDataFrame(evAll)
     eaDF.registerTempTable("evall")



    val mm = sqlContext.sql(" select mm.*, allev  from mm LEFT OUTER JOIN evall ON mm.device = evall.device_id")
     mm.registerTempTable("nn")




    val evLat = new h2o.H2OFrame( new URI("file:///" + SparkFiles.get(evNolat)))
     val elDF = asDataFrame(evLat)
     elDF.registerTempTable("evlat")


    val nn = sqlContext.sql(" select nn.*, nolat  from nn LEFT OUTER JOIN evlat ON nn.device = evlat.device_id")
    nn.registerTempTable("oo")



//    val clust = new h2o.H2OFrame( new URI("file:///" + SparkFiles.get(clustfile)))
//     val clustDF = asDataFrame(clust)
//    clustDF.registerTempTable("clust")
//
//
//    val clu = (0 until EventsClusteringProcessing.K).map(i => "c"+i.toString).mkString(",")
//    println(clu)
//    val oo = sqlContext.sql(" select oo.*, "+clu+"  from oo LEFT OUTER JOIN clust ON oo.device = oo.device")
//
//    println(clu)
//    println("APPS:"+ tDF.count +" TOGETHER:" + oo.count)
//    oo.take(20).foreach(println)


//
//    val myFullData = new h2o.H2OFrame(lineBuilder(
//      oo, Array(0, 3, 4, 5), "Apps"
//    ))
//
//    println(s" AND MY DATA IS: ${myFullData.key} =>  ${myFullData.numCols()} / ${myFullData.numRows()}")
//
//    Helper.saveCSV(myFullData, output_full_filename)
    Helper.saveCSV( asH2OFrame(nn), output_full_filename)


    println(
      s"""
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         |!!  OUTPUT CREATED: ${output_full_filename} !!
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         """.stripMargin)

    //clean
    println("... and cleaned")

  }


  //@TODO: Dropping constant columns:


  def lineBuilder(out: DataFrame, str: Array[Int], name: String): Frame = {

    val headers = out.columns
    val startIdx = 6
    val len = headers.length
    println("LEN::" + len)

    val fs = new Array[Futures](len)
    val av = new Array[AppendableVec](len)
    val chunks = new Array[NewChunk](len)
    val vecs = new Array[Vec](len)


    for (i <- 0 until len) {
      fs(i) = new Futures()
      if (str.contains(i))
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_STR)
      else
        av(i) = new AppendableVec(new Vec.VectorGroup().addVec(), Vec.T_NUM)
      chunks(i) = new NewChunk(av(i), 0)
    }

    println("Structure is there")
    out.collect.foreach(ae => {
      //collect

      //println("AND::" + ae.get(0)+ "::"+ae.get(1) +"::"+ae.get(2))
      chunks(0).addStr(ae.getLong(0).toString)
      if (ae.isNullAt(1)) {
        chunks(1).addNA()
      }
      else {
        chunks(1).addNum(ae.getByte(1))
      }
      if (ae.isNullAt(2)) {
        chunks(2).addNA()
      }
      else {
        chunks(2).addNum(ae.getByte(2)) //age
      }
      chunks(3).addStr(ae.getString(3))
      chunks(4).addStr(ae.getString(4))
      if (ae.isNullAt(5)) {
        chunks(5).addNA()
      }
      else {
        chunks(5).addStr(ae.getString(5))
      }

      for (i <- 6 until len) {
        if (ae.isNullAt(i)) {
          chunks(i).addNA()
        }
        else {
          try {
            chunks(i).addNum(ae.getByte(i))
          } catch {
            case ss: ClassCastException => {
              try {
                chunks(i).addNum(ae.getShort(i))
              } catch {
                case bs: ClassCastException => {
                  chunks(i).addNum(ae.getInt(i))
                }
              }
            }
          }
        }
      }

    })

    println("Finalize")

    for (i <- 0 until len) {
      chunks(i).close(0, fs(i))
      vecs(i) = av(i).layout_and_close(fs(i))
      fs(i).blockForPending()
    }

    val key = Key.make(name)
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
