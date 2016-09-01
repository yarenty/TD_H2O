package com.yarenty.td.normalized

import java.io.File

import com.yarenty.td.schemas.{EventCSVParser, _}
import com.yarenty.td.utils.Helper
import hex.Distribution
import hex.kmeans.{KMeans, KMeansModel}
import hex.kmeans.KMeansModel.KMeansParameters
import hex.naivebayes.{NaiveBayes, NaiveBayesModel}
import hex.naivebayes.NaiveBayesModel.NaiveBayesParameters
import org.apache.spark._
import org.apache.spark.h2o._
import org.apache.spark.sql.SQLContext
import water._
import water.fvec._
import water.support.SparkContextSupport

import scala.collection.mutable.Map


/**
  * Created by yarenty on 24/08/2016.
  * (C)2015 SkyCorp Ltd.
  */
object EventsClusteringProcessing extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val events = "events.csv"
  val output_filename = "/opt/data/TalkingData/model/events_clusters"
  val nolat_out = "/opt/data/TalkingData/model/events_nolat"
  val noEvents_out = "/opt/data/TalkingData/model/events_noEvents"
  val clustfile = "/opt/data/TalkingData/model/events_time_cluster"



  val K=200

  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + events)
    )



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)


    val eventData = new h2o.H2OFrame(EventClusterCSVParser.get, new File(SparkFiles.get(events)))
    println(s"\n===> eventData via H2O#Frame#count: ${eventData.numRows}\n")

    val eventDF = asDataFrame(eventData)
    eventDF.registerTempTable("events")
//
//    val tpattern = sqlContext.sql(" select device_id, count(*) as nolat from events " +
//       " where latitude=0 and longitude=0 group by device_id ")
//    tpattern.head(20).foreach(println)
//
//    Helper.saveCSV(tpattern, nolat_out)
//
//
//    val epattern = sqlContext.sql(" select device_id, count(*) as allev from events " +
//       " group by device_id ")
//    epattern.head(20).foreach(println)
//    Helper.saveCSV(epattern, noEvents_out)

    val model = kModel(eventData)
    val red = model.score(eventData)

//    val out = eventData.subframe(Array("device_id"))
//    out.add("kluster",red.vec("predict"))
     red.add("device_id",eventData.vec("device_id"))


    val cl = asDataFrame(red)
    cl.registerTempTable("klaster")

    val kpattern =  sqlContext.sql(" select device_id, predict as kluster, count(*) as ktimes from klaster " +
           " group by device_id, predict ")

    kpattern.registerTempTable("kpattern")


    val oMap: Map[Long, Map[Int, Int]] = Map[Long, Map[Int, Int]]()



    val z:scala.collection.Map[Long, Map[Int, Int]] = kpattern.flatMap(row => {

         val did: Long = row.getAs[String]("device_id").toLong
         val ts = row.getAs[Short]("kluster").toInt
         val c = row.getAs[Long]("ktimes").toInt


         if (oMap.contains(did)) {
           oMap.get(did).get += (ts -> c)
         } else {
           val nm = Map(ts -> c)
           oMap += (did -> nm)
         }
      oMap
    }).collectAsMap

    Helper.saveCSV(kpattern, clustfile)


    val myData = new h2o.H2OFrame(lineBuilder(z))

    println(s" AND MY DATA IS: ${myData.key} =>  ${myData.numCols()} / ${myData.numRows()}")


    Helper.saveCSV(myData, output_filename)

    println(
      s"""
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         |!!  OUTPUT CREATED: ${output_filename} !!
         |!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
         """.stripMargin)

    //clean
//    eventData.delete()
    println("... and cleaned")

  }


  def kModel(train: H2OFrame): KMeansModel = {
    //logloss = 3.65712

    val params = new KMeansParameters()
    params._train = train.key
    params._distribution = Distribution.Family.gaussian
    params._ignored_columns = Array("event_id","device_id","timestamp")
    params._ignore_const_cols = true
    params._k=K
    println("BUILDING:" + params.fullName)
    val dl = new KMeans(params)
    dl.trainModel.get
  }


  def kTimeModel(train: H2OFrame): KMeansModel = {
    //logloss = 3.65712

    val params = new KMeansParameters()
    params._train = train.key
    params._distribution = Distribution.Family.gaussian
    params._ignored_columns = Array("event_id","device_id","timestamp")
    params._ignore_const_cols = true
    params._k=K
    println("BUILDING:" + params.fullName)
    val dl = new KMeans(params)
    dl.trainModel.get
  }







  def lineBuilder( oMap:scala.collection.Map[Long, Map[Int, Int]]): Frame = {

    val headers = Array(
      "device") ++ (0 until K).map(i => "c" + i.toString)
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
        for (i <- 0 until K) {
          if (dM.contains(i)) {
            chunks(i+1).addNum(
              dM.get(i).get.toDouble)
          } else {
            chunks(i+1).addNum(0)
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
