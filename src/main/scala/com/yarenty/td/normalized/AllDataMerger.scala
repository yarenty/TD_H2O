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



    val clust = new h2o.H2OFrame( new URI("file:///" + SparkFiles.get(clustfile)))
     val clustDF = asDataFrame(clust)
    clustDF.registerTempTable("clust")


    val clu = (0 until EventsClusteringProcessing.K).map(i => "c"+i.toString).mkString(",")
    println(clu)
    val oo = sqlContext.sql(" select oo.*, "+clu+"  from oo LEFT OUTER JOIN clust ON oo.device = clust.device")

    println(clu)
    println("APPS:"+ tDF.count +" TOGETHER:" + oo.count)
    oo.take(20).foreach(println)



    val myFullData = new h2o.H2OFrame(lineBuilder(
      oo, Array(0, 3, 4, 5), "Apps"
    ))

    println(s" AND MY DATA IS: ${myFullData.key} =>  ${myFullData.numCols()} / ${myFullData.numRows()}")

    Helper.saveCSV(myFullData, output_full_filename)
//    Helper.saveCSV( asH2OFrame(oo), output_full_filename)


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



  val normtra = Array(0.0,1.0,0.0,1.0,0.0,2.0,2.0,4.0,5.0,4.0,5.0,5.0,8.0,3.0,0.0,9.0,5.0,3.0,1.0,1.0,1.0,7.0,1.0,2.0,1.0,2.0,5.0,0.0,5.0,3.0,5.0,4.0,3.0,0.0,2.0,4.0,1.0,3.0,2.0,2.0,1.0,8.0,6.0,4.0,3.0,3.0,9.0,1.0,2.0,3.0,73.0,6.0,1.0,7.0,1.0,1.0,2.0,2.0,2.0,2.0,2.0,1.0,1.0,2.0,4.0,3.0,0.0,1.0,4.0,3.0,1.0,2.0,2.0,1.0,1.0,2.0,3.0,2.0,2.0,2.0,1.0,1.0,1.0,4.0,1.0,3.0,1.0,5.0,2.0,5.0,1.0,3.0,2.0,1.0,5.0,2.0,3.0,4.0,5.0,16.0,3.0,1.0,3.0,6.0,3.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,22.0,7.0,14.0,7.0,2.0,0.0,49.0,11.0,2.0,12.0,7.0,2.0,2.0,1.0,8.0,0.0,2.0,0.0,0.0,10.0,6.0,6.0,8.0,6.0,16.0,3.0,3.0,0.0,3.0,1.0,10.0,5.0,1.0,6.0,3.0,6.0,4.0,0.0,6.0,9.0,17.0,4.0,3.0,0.0,12.0,1.0,1.0,2.0,0.0,9.0,15.0,22.0,8.0,4.0,0.0,7.0,2.0,2.0,8.0,6.0,9.0,6.0,5.0,6.0,2.0,11.0,3.0,2.0,0.0,5.0,2.0,1.0,1.0,2.0,0.0,4.0,10.0,2.0,9.0,6.0,0.0,24.0,1.0,8.0,4.0,2.0,2.0,4.0,1.0,1.0,1.0,5.0,2.0,5.0,12.0,7.0,3.0,2.0,1.0,7.0,0.0,14.0,2.0,6.0,3.0,4.0,2.0,3.0,8.0,6.0,2.0,0.0,2.0,2.0,1.0,0.0,0.0,1.0,1.0,2.0,0.0,1.0,1.0,66.0,41.0,18.0,16.0,4.0,24.0,2.0,2.0,2.0,4.0,10.0,19.0,20.0,0.0,1.0,8.0,3.0,1.0,2.0,1.0,3.0,10.0,3.0,1.0,2.0,1.0,9.0,0.0,5.0,6.0,3.0,1.0,2.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,53.0,33.0,0.0,0.0,31.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,4.0,4.0,8.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,11.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,135.0,13.0,13.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,185.0,105.0,0.0,9.0,8.0,1.0,0.0,8.0,0.0,0.0,4.0,8.0,0.0,0.0,17.0,1.0,21.0,4.0,3.0,0.0,2.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,2.0,2.0,2.0,18.0,2.0,0.0,3.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,126.0,7.0,16.0,8.0,3.0,4.0,68.0,11.0,2.0,76.0,41.0,13.0,2.0,5.0,14.0,14.0,13.0,26.0,1.0,14.0,14.0,0.0,0.0,0.0,0.0,0.0,134.0,30.0,30.0,0.0,1.0,0.0,1.0,23.0,21.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,7.0,4.0,4.0,0.0,22.0,19.0,0.0,0.0,13.0,75.0,36.0,20.0,13.0,2.0,42.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,12.0,12.0,2.0,13.0,45.0,47.0,5.0,45.0,31.0,36.0,18.0,34.0,46.0,47.0,0.0,7.0,46.0,45.0,7.0,12.0,4.0,12.0,3.0,1.0,20.0,15.0,5.0,3.0,3.0,0.0,4.0,1.0,5.0,2.0,2.0,2.0,4.0,2.0,1.0,4.0,4.0,9.0,3.0,4.0,6.0,3.0,1.0,5.0,6.0,2.0,3.0,2.0,1.0,3.0,1.0,10.0,9.0,2.0,1.0,3.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,3.0,126.0,4.0,126.0,6.0,5.0,80.0,47.0,6.0,0.0,0.0,0.0,0.0,0.0,0.0,25.0,1.0,2.0,10.0,2.0,19.0,2.0,4.0,11.0,3.0,3.0,5.0,1.0,5.0,4.0,3.0,17.0,11.0,4.0,6.0,10.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,26.0,13.0,0.0,0.0,0.0,0.0,0.0,13.0,9.0,4.0,3.0,0.0,0.0,3.0,3.0,0.0,1.0,13.0,13.0,2.0,0.0,2.0,3.0,21.0,13.0,3.0,8.0,3.0,9.0,4.0,9.0,9.0,3.0,3.0,6.0,1.0,6.0,3.0,1.0,16.0,3.0,4.0,4.0,3.0,11.0,6.0,6.0,4.0,6.0,4.0,85.0,85.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,4.0,1.0,1.0,1.0,1.0,2.0,1.0,2.0,1.0,3.0,0.0,1.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,0.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,10.0,0.0,10.0,10.0,100.0,18.0,5.0,8.0,11.0,37.0,14.0,42.0,21.0,4.0,19.0,19.0,17.0,7.0,2.0,73.0,98.0,117.0,90.0,60.0,56.0,66.0,47.0,45.0,41.0,48.0,44.0,32.0,47.0,35.0,35.0,37.0,38.0,36.0,53.0,29.0,39.0,30.0,35.0,41.0,39.0,35.0,28.0,41.0,48.0,42.0,37.0,56.0,57.0,76.0,87.0,48.0,76.0,91.0,61.0,52.0,68.0,80.0,103.0,74.0,85.0,57.0,54.0,41.0,49.0,65.0,73.0,63.0,70.0,53.0,64.0,44.0,50.0,71.0,154.0,124.0,133.0,150.0,117.0,76.0,52.0,63.0,54.0,57.0,55.0,45.0,58.0,42.0,46.0,41.0,41.0,39.0,39.0,36.0,37.0,38.0,52.0,37.0,48.0,46.0,58.0,74.0,45.0,43.0,51.0,33.0,55.0,33.0,43.0,47.0,43.0,53.0,40.0,45.0,74.0,64.0,60.0,47.0,61.0,42.0,34.0,71.0,57.0,76.0,66.0,58.0,44.0,68.0,48.0,63.0,61.0,66.0,63.0,57.0,53.0,40.0,53.0,91.0,97.0,128.0,105.0,114.0,68.0,55.0,60.0,48.0,59.0,65.0,58.0,54.0,67.0,75.0,62.0,65.0,48.0,52.0,55.0,55.0,4150.0,611.0,611.0,0.0,0.0,0.0,36.0,0.0,63.0,0.0,3.0,0.0,0.0,5.0,0.0,0.0,0.0,0.0,7.0,0.0,0.0,0.0,0.0,0.0,0.0,10.0,0.0,0.0,6.0,0.0,0.0,40.0,96.0,0.0,28.0,0.0,5.0,0.0,0.0,84.0,0.0,0.0,9.0,0.0,62.0,0.0,0.0,0.0,10.0,1276.0,37.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,457.0,20.0,0.0,430.0,0.0,0.0,75.0,0.0,763.0,242.0,0.0,215.0,7.0,0.0,297.0,0.0,705.0,0.0,8.0,254.0,18.0,0.0,0.0,196.0,5.0,28.0,0.0,113.0,2.0,0.0,396.0,0.0,163.0,0.0,0.0,143.0,0.0,0.0,2.0,0.0,1.0,4.0,17.0,135.0,147.0,0.0,57.0,0.0,102.0,0.0,40.0,363.0,0.0,196.0,0.0,0.0,0.0,0.0,135.0,0.0,68.0,7.0,159.0,455.0,78.0,0.0,19.0,37.0,26.0,31.0,97.0,0.0,0.0,0.0,186.0,212.0,85.0,21.0,44.0,33.0,0.0,118.0,299.0,19.0,516.0,411.0,32.0,489.0,0.0,0.0,121.0,4.0,12.0,0.0,4.0,185.0,7.0,9.0,5.0,26.0,2.0,40.0,26.0,41.0,21.0,0.0,0.0,0.0,74.0,328.0,0.0,184.0,0.0,82.0,56.0,0.0,0.0,724.0,0.0,342.0,61.0,0.0,0.0,2.0,40.0,35.0,32.0,0.0,0.0,527.0,0.0,4.0,4.0,0.0,0.0,161.0,0.0,5.0,334.0,0.0,0.0,0.0,36.0,35.0,20.0,30.0,0.0,13.0,0.0,18.0,6.0,162.0,99.0,0.0,128.0,344.0,64.0,0.0,90.0,181.0,0.0,0.0,771.0,1.0,252.0,258.0,12.0,194.0,14.0,62.0,23.0,248.0,40.0,909.0,0.0,1127.0,0.0,0.0,103.0,0.0,7.0,0.0,223.0,37.0,85.0,0.0,16.0,522.0,138.0,696.0,14.0,80.0,50.0,2.0,68.0,19.0,81.0,52.0,349.0,677.0,814.0,0.0,249.0,0.0,1912.0,8.0,0.0,0.0,38.0,6.0,475.0,72.0,777.0,1.0,399.0,0.0,60.0,52.0,83.0,91.0,327.0,17.0,0.0,72.0,23.0,671.0,49.0,18.0,134.0,8.0,60.0,88.0,202.0,0.0,0.0,38.0,109.0,11.0,143.0,354.0,2.0,67.0,291.0,0.0,88.0,25.0,164.0,296.0,51.0,36.0,20.0,37.0,0.0,102.0,15.0,46.0,0.0,19.0,10.0,0.0,12.0,97.0,354.0,71.0,0.0,225.0,0.0,305.0,0.0,0.0,225.0,55.0,30.0,1.0,4.0,8.0,656.0,0.0,376.0,129.0,0.0,615.0,8.0,0.0,97.0,76.0,69.0,0.0,0.0,129.0,349.0,61.0,0.0,52.0,0.0,6.0,69.0,0.0,992.0,0.0,0.0,45.0,0.0,8.0,0.0,0.0,3.0,0.0,68.0,0.0,57.0,95.0,181.0,0.0,0.0,49.0,0.0,18.0,16.0,44.0,62.0,0.0,0.0,116.0,0.0,4.0,6.0,351.0,91.0,98.0,74.0,73.0,77.0,74.0,93.0,81.0,8.0,195.0,247.0,256.0,299.0,314.0,818.0,806.0,266.0,98.0,230.0,202.0,0.0,158.0,35.0,20.0,0.0,63.0,3946.0,70.0,102.0,76.0,8.0,1345.0,1.0,0.0,888.0,292.0,6.0,189.0,78.0,0.0,99.0,0.0,11.0,0.0,928.0,37.0,151.0,264.0,27.0,88.0,611.0,0.0,90.0,477.0,126.0,0.0,114.0,181.0,8.0,173.0,853.0,31.0,15.0,74.0,152.0,76.0,10.0,88.0,377.0,14.0,78.0,52.0,2753.0,109.0,66.0,49.0,380.0,231.0,50.0,0.0,0.0,0.0,161.0,116.0,573.0,861.0,23.0,3.0,125.0,100.0,382.0,23.0,120.0,277.0,0.0,0.0,14.0,0.0,0.0,7.0,31.0,804.0,23.0,68.0,430.0,3.0,63.0,157.0,177.0,7.0,206.0,258.0,16.0,10.0,144.0,0.0,329.0,0.0,379.0,155.0,14.0,126.0,0.0,334.0,157.0,10.0,388.0,0.0,114.0,0.0,116.0,342.0,259.0,47.0,474.0,0.0,189.0,34.0,35.0,58.0,50.0,0.0,17.0,0.0,27.0,38.0,0.0,172.0,8.0,257.0,122.0,0.0,64.0,698.0,11.0,0.0,26.0,14.0,191.0,0.0,2.0,0.0,0.0,175.0,169.0,63.0,4.0,0.0,394.0,32.0,275.0,13.0,1.0,89.0,9.0,27.0,0.0,0.0,0.0,4.0,74.0,8.0,0.0,10.0,99.0,0.0,0.0,27.0,40.0,0.0,0.0,0.0,860.0,125.0,729.0,0.0,5.0,0.0,0.0,124.0,7.0,0.0,0.0,56.0,0.0,58.0,739.0,0.0,5.0,11.0,6.0,110.0,251.0,23.0,86.0,300.0,178.0,1253.0,247.0,488.0,64.0,197.0,95.0,0.0,383.0,133.0,666.0,145.0,2.0,146.0,7.0,0.0,35.0,847.0,0.0,0.0,80.0,436.0,18.0,3.0,1279.0,19.0,0.0,64.0,34.0,121.0,1052.0,313.0,11.0,660.0,0.0,257.0,108.0,26.0,38.0,103.0,4.0,90.0,220.0,32.0,146.0,168.0,855.0,8.0,243.0,846.0,4.0,4.0,173.0,384.0,442.0,0.0,93.0,144.0,0.0,0.0,87.0,0.0,122.0,54.0,99.0,61.0,531.0,135.0,292.0,342.0,122.0,696.0,112.0,160.0,54.0,0.0,232.0,46.0,62.0,150.0,633.0,13.0,10.0,813.0,241.0,0.0,0.0,0.0,130.0,0.0,347.0,53.0,24.0,5.0,0.0,0.0,104.0,0.0,3.0,105.0,4.0,0.0,45.0,1090.0,189.0,0.0,361.0,4.0,102.0,168.0,4.0,4.0,351.0,0.0,3.0,52.0,0.0,51.0,0.0,4.0,31.0,185.0,100.0,28.0,231.0,77.0,0.0,467.0,4.0,36.0,2.0,0.0,0.0,33.0,0.0,20.0,6.0,0.0,271.0,0.0,173.0,30.0,7.0,60.0,165.0,600.0,116.0,89.0,4.0,31.0,4.0,15.0,44.0,49.0,0.0,0.0,372.0,13.0,784.0,87.0,17.0,0.0,18.0,41.0,0.0,9.0,182.0,131.0,0.0,34.0,5.0,21.0,587.0,18.0,55.0,382.0,229.0,0.0,31.0,34.0,310.0,0.0,49.0,0.0,6.0,0.0,14.0,167.0,45.0,0.0,81.0,48.0,785.0,125.0,15.0,240.0,85.0,301.0,198.0,66.0,197.0,295.0,39.0,447.0,245.0,0.0,229.0,2893.0,67.0,97.0,0.0,4.0,13.0,20.0,7.0,275.0,708.0,0.0,32.0,884.0,49.0,0.0,121.0,0.0,73.0,0.0,1.0,155.0,334.0,237.0,30.0,266.0,24.0,673.0,21.0,24.0,89.0,72.0,379.0,40.0,37.0,165.0,180.0,94.0,3.0,0.0,1299.0,4.0,302.0,4.0,0.0,91.0,218.0,360.0,0.0,0.0,20.0,0.0,204.0,112.0,187.0,158.0,362.0,16.0,38.0,0.0,885.0,174.0,325.0,16.0,12.0,20.0,30.0,32.0,206.0,17.0,414.0,204.0,0.0,28.0,0.0,301.0,108.0,119.0,413.0,97.0,6.0,267.0,68.0,811.0,343.0,0.0,17.0,122.0,84.0,47.0,18.0,10.0,182.0,0.0,4.0,90.0,32.0,53.0,6.0,100.0,35.0,4.0,342.0,0.0,0.0,468.0,291.0,68.0,117.0,121.0,47.0,353.0,177.0,441.0,126.0,0.0,179.0,0.0,191.0,206.0,74.0,0.0,62.0,31.0,3.0,78.0,14.0,12.0,470.0,220.0,1500.0,411.0,0.0,8.0,16.0,0.0,49.0,26.0,58.0,10.0,21.0,9.0,0.0,460.0,15.0,32.0,0.0,439.0,16.0,0.0,13.0,95.0,0.0,6.0,0.0,44.0,95.0,206.0,169.0,0.0,36.0,3128.0,111.0,0.0,6.0,56.0,5.0,0.0,123.0,45.0,35.0,40.0,0.0,176.0,45.0,19.0,31.0,0.0,83.0,5.0,0.0,0.0,56.0,31.0,0.0,260.0,0.0,0.0)
  val normtst = Array(0.0,1.0,0.0,1.0,1.0,10.0,5.0,49.0,23.0,23.0,31.0,24.0,158.0,36.0,0.0,116.0,37.0,17.0,3.0,1.0,1.0,39.0,4.0,15.0,3.0,10.0,38.0,0.0,21.0,26.0,25.0,15.0,24.0,0.0,9.0,49.0,4.0,19.0,2.0,6.0,1.0,63.0,30.0,22.0,6.0,6.0,17.0,0.0,3.0,4.0,101.0,10.0,0.0,57.0,1.0,2.0,17.0,3.0,2.0,2.0,5.0,3.0,1.0,12.0,19.0,14.0,1.0,1.0,54.0,19.0,0.0,10.0,10.0,1.0,2.0,5.0,16.0,8.0,2.0,6.0,1.0,1.0,1.0,21.0,3.0,21.0,3.0,18.0,7.0,18.0,3.0,7.0,4.0,1.0,14.0,6.0,7.0,39.0,27.0,136.0,28.0,8.0,13.0,54.0,19.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,55.0,9.0,35.0,6.0,1.0,0.0,64.0,51.0,2.0,61.0,21.0,2.0,5.0,1.0,45.0,0.0,6.0,0.0,0.0,17.0,24.0,17.0,20.0,28.0,59.0,9.0,5.0,1.0,4.0,1.0,36.0,8.0,2.0,6.0,4.0,11.0,10.0,0.0,13.0,19.0,81.0,12.0,6.0,0.0,28.0,1.0,1.0,1.0,0.0,19.0,44.0,148.0,15.0,14.0,0.0,17.0,3.0,3.0,9.0,6.0,9.0,14.0,9.0,7.0,2.0,13.0,2.0,3.0,0.0,6.0,2.0,1.0,2.0,3.0,0.0,6.0,15.0,5.0,29.0,21.0,0.0,64.0,2.0,11.0,9.0,2.0,3.0,9.0,1.0,3.0,1.0,8.0,3.0,8.0,13.0,9.0,6.0,4.0,1.0,19.0,0.0,18.0,3.0,9.0,7.0,4.0,3.0,8.0,10.0,4.0,3.0,0.0,2.0,3.0,1.0,0.0,0.0,1.0,1.0,3.0,0.0,1.0,1.0,105.0,47.0,15.0,22.0,4.0,36.0,2.0,2.0,3.0,5.0,11.0,28.0,25.0,0.0,0.0,14.0,2.0,1.0,2.0,1.0,3.0,15.0,5.0,1.0,3.0,1.0,12.0,0.0,7.0,5.0,9.0,2.0,3.0,2.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,411.0,203.0,0.0,0.0,221.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,8.0,7.0,15.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,18.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,499.0,14.0,14.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1982.0,628.0,0.0,10.0,9.0,1.0,0.0,9.0,0.0,0.0,4.0,9.0,0.0,0.0,21.0,1.0,33.0,3.0,4.0,0.0,3.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,2.0,2.0,1.0,23.0,2.0,0.0,3.0,2.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,961.0,13.0,37.0,27.0,6.0,8.0,156.0,98.0,2.0,276.0,322.0,37.0,3.0,12.0,13.0,63.0,14.0,109.0,5.0,46.0,32.0,0.0,0.0,0.0,0.0,0.0,177.0,33.0,31.0,0.0,1.0,0.0,1.0,16.0,12.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,8.0,7.0,3.0,0.0,28.0,26.0,0.0,0.0,16.0,108.0,42.0,22.0,12.0,2.0,48.0,3.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,6.0,6.0,1.0,14.0,34.0,67.0,5.0,65.0,40.0,42.0,15.0,34.0,55.0,78.0,0.0,8.0,45.0,54.0,8.0,24.0,5.0,23.0,3.0,1.0,321.0,289.0,21.0,14.0,15.0,0.0,16.0,2.0,14.0,11.0,3.0,15.0,8.0,4.0,4.0,22.0,50.0,69.0,17.0,29.0,7.0,3.0,1.0,5.0,16.0,2.0,6.0,4.0,1.0,3.0,2.0,11.0,8.0,2.0,2.0,3.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,4.0,126.0,8.0,112.0,9.0,12.0,63.0,39.0,12.0,0.0,0.0,0.0,0.0,0.0,0.0,690.0,3.0,2.0,232.0,1.0,147.0,5.0,54.0,47.0,5.0,5.0,14.0,1.0,43.0,19.0,15.0,101.0,86.0,60.0,123.0,185.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,56.0,10.0,0.0,0.0,0.0,0.0,0.0,9.0,6.0,3.0,1.0,0.0,0.0,4.0,4.0,1.0,1.0,9.0,8.0,2.0,0.0,1.0,1.0,53.0,14.0,4.0,9.0,2.0,8.0,4.0,9.0,9.0,3.0,3.0,21.0,5.0,19.0,2.0,2.0,22.0,3.0,4.0,10.0,3.0,10.0,8.0,8.0,5.0,5.0,6.0,124.0,124.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,6.0,48.0,5.0,7.0,1.0,1.0,13.0,7.0,3.0,3.0,8.0,0.0,1.0,1.0,1.0,1.0,2.0,0.0,1.0,1.0,1.0,0.0,2.0,1.0,0.0,1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,17.0,0.0,20.0,18.0,154.0,20.0,4.0,9.0,13.0,35.0,24.0,48.0,34.0,6.0,18.0,9.0,17.0,8.0,3.0,255.0,178.0,136.0,111.0,132.0,109.0,158.0,123.0,127.0,124.0,102.0,105.0,121.0,100.0,90.0,77.0,88.0,89.0,112.0,87.0,103.0,86.0,87.0,85.0,86.0,78.0,79.0,83.0,78.0,86.0,138.0,137.0,117.0,117.0,129.0,149.0,185.0,185.0,144.0,170.0,218.0,205.0,218.0,203.0,186.0,198.0,164.0,158.0,226.0,176.0,221.0,201.0,208.0,176.0,280.0,218.0,192.0,186.0,183.0,226.0,423.0,263.0,281.0,273.0,205.0,243.0,374.0,275.0,202.0,229.0,251.0,259.0,331.0,254.0,231.0,231.0,239.0,247.0,325.0,279.0,301.0,276.0,270.0,252.0,299.0,288.0,290.0,270.0,255.0,241.0,238.0,291.0,241.0,226.0,207.0,203.0,245.0,211.0,219.0,216.0,245.0,234.0,282.0,277.0,297.0,257.0,265.0,249.0,317.0,273.0,333.0,349.0,354.0,367.0,490.0,422.0,385.0,379.0,366.0,420.0,468.0,417.0,362.0,341.0,353.0,396.0,474.0,408.0,458.0,404.0,319.0,292.0,399.0,319.0,296.0,294.0,289.0,224.0,314.0,283.0,261.0,189.0,148.0,33426.0,7132.0,1253.0,14.0,7.0,0.0,125.0,41.0,0.0,0.0,0.0,370.0,49.0,220.0,0.0,0.0,159.0,96.0,42.0,31.0,0.0,4.0,0.0,0.0,2.0,0.0,0.0,4.0,11.0,118.0,25.0,0.0,0.0,0.0,0.0,0.0,0.0,22.0,0.0,0.0,0.0,0.0,134.0,0.0,676.0,40.0,0.0,2.0,0.0,914.0,141.0,0.0,2.0,0.0,0.0,0.0,0.0,0.0,2.0,10.0,11.0,0.0,0.0,0.0,133.0,144.0,4.0,376.0,0.0,51.0,0.0,0.0,2779.0,386.0,0.0,189.0,85.0,6.0,0.0,0.0,942.0,0.0,0.0,775.0,23.0,4.0,0.0,434.0,60.0,42.0,0.0,899.0,0.0,0.0,290.0,78.0,158.0,0.0,0.0,62.0,0.0,6.0,0.0,0.0,18.0,6.0,0.0,166.0,58.0,9.0,29.0,0.0,180.0,17.0,253.0,772.0,87.0,198.0,0.0,0.0,71.0,105.0,83.0,0.0,0.0,0.0,39.0,238.0,0.0,12.0,16.0,43.0,94.0,26.0,170.0,0.0,0.0,0.0,391.0,881.0,363.0,17.0,29.0,39.0,6.0,139.0,341.0,0.0,411.0,1767.0,0.0,32.0,16.0,204.0,133.0,17.0,74.0,5.0,0.0,598.0,204.0,89.0,299.0,161.0,946.0,153.0,186.0,20.0,255.0,0.0,181.0,10.0,1370.0,453.0,0.0,165.0,71.0,265.0,189.0,0.0,0.0,10.0,4.0,359.0,188.0,0.0,16.0,77.0,0.0,36.0,86.0,0.0,0.0,802.0,4.0,0.0,52.0,0.0,0.0,15.0,2.0,52.0,215.0,254.0,0.0,0.0,86.0,128.0,0.0,50.0,0.0,4.0,7.0,4.0,51.0,43.0,0.0,0.0,243.0,230.0,78.0,1.0,182.0,281.0,4.0,285.0,434.0,39.0,219.0,476.0,350.0,308.0,8.0,31.0,0.0,257.0,7.0,697.0,0.0,341.0,2.0,62.0,23.0,121.0,0.0,385.0,296.0,59.0,80.0,35.0,272.0,190.0,324.0,248.0,186.0,1743.0,210.0,24.0,0.0,0.0,81.0,42.0,992.0,285.0,776.0,1.0,147.0,0.0,208.0,21.0,0.0,6.0,56.0,4.0,400.0,280.0,2936.0,27.0,2625.0,88.0,34.0,338.0,339.0,173.0,157.0,10.0,11.0,369.0,4.0,0.0,32.0,139.0,827.0,15.0,73.0,4.0,203.0,33.0,9.0,101.0,104.0,362.0,264.0,319.0,448.0,161.0,139.0,0.0,110.0,86.0,164.0,271.0,410.0,47.0,0.0,26.0,1.0,523.0,8.0,57.0,29.0,13.0,143.0,0.0,3.0,68.0,0.0,26.0,0.0,135.0,5.0,26.0,3.0,0.0,1992.0,112.0,30.0,47.0,1.0,96.0,295.0,33.0,64.0,358.0,0.0,94.0,16.0,0.0,755.0,40.0,143.0,16.0,35.0,42.0,988.0,107.0,0.0,0.0,4.0,16.0,31.0,866.0,111.0,0.0,78.0,5.0,122.0,0.0,4.0,0.0,133.0,0.0,347.0,3.0,191.0,290.0,402.0,0.0,0.0,5.0,2.0,37.0,54.0,98.0,96.0,0.0,5.0,130.0,68.0,4.0,40.0,63.0,119.0,72.0,371.0,283.0,82.0,23.0,11.0,145.0,0.0,143.0,442.0,51.0,108.0,1546.0,1817.0,538.0,162.0,126.0,509.0,108.0,119.0,82.0,0.0,173.0,36.0,311.0,207.0,338.0,7.0,515.0,0.0,813.0,2.0,33.0,1079.0,130.0,776.0,21.0,54.0,10.0,123.0,0.0,11.0,2.0,773.0,2.0,364.0,720.0,22.0,27.0,65.0,134.0,0.0,251.0,405.0,123.0,237.0,370.0,192.0,88.0,551.0,124.0,12.0,143.0,317.0,13.0,231.0,1324.0,431.0,652.0,148.0,42.0,693.0,200.0,496.0,595.0,286.0,293.0,17.0,36.0,0.0,19.0,138.0,19.0,675.0,32.0,113.0,1131.0,122.0,260.0,20.0,260.0,154.0,293.0,8.0,0.0,7.0,4.0,8.0,556.0,77.0,0.0,63.0,65.0,65.0,158.0,224.0,423.0,25.0,438.0,341.0,10.0,0.0,6.0,188.0,0.0,273.0,0.0,441.0,93.0,47.0,121.0,20.0,6.0,334.0,84.0,239.0,9.0,41.0,0.0,261.0,42.0,149.0,93.0,295.0,1.0,66.0,2.0,316.0,61.0,63.0,0.0,0.0,21.0,36.0,46.0,0.0,39.0,86.0,4.0,214.0,0.0,270.0,487.0,55.0,0.0,144.0,75.0,254.0,0.0,5.0,6.0,2.0,76.0,176.0,12.0,0.0,0.0,117.0,0.0,254.0,7.0,286.0,170.0,200.0,393.0,0.0,1.0,0.0,3.0,230.0,232.0,0.0,162.0,710.0,0.0,94.0,107.0,10.0,318.0,0.0,0.0,236.0,33.0,1028.0,16.0,0.0,17.0,2.0,2.0,6.0,4.0,0.0,115.0,24.0,121.0,649.0,148.0,456.0,8.0,88.0,444.0,459.0,318.0,2.0,831.0,3.0,2890.0,63.0,348.0,38.0,518.0,337.0,0.0,157.0,247.0,12.0,70.0,259.0,63.0,310.0,47.0,352.0,402.0,2.0,5.0,6.0,2341.0,348.0,120.0,327.0,26.0,7.0,189.0,26.0,262.0,1781.0,1522.0,64.0,927.0,0.0,86.0,474.0,88.0,70.0,0.0,4.0,118.0,106.0,364.0,146.0,33.0,438.0,18.0,442.0,944.0,210.0,6.0,428.0,648.0,78.0,74.0,36.0,0.0,11.0,12.0,13.0,0.0,402.0,173.0,109.0,252.0,650.0,357.0,236.0,843.0,131.0,200.0,70.0,641.0,162.0,6.0,1166.0,137.0,199.0,224.0,709.0,76.0,0.0,1222.0,243.0,69.0,5.0,34.0,143.0,332.0,6.0,7.0,149.0,44.0,14.0,30.0,640.0,0.0,0.0,95.0,4.0,28.0,341.0,2513.0,32.0,0.0,264.0,4.0,733.0,33.0,59.0,139.0,55.0,0.0,40.0,52.0,6.0,54.0,21.0,88.0,114.0,856.0,315.0,42.0,316.0,78.0,0.0,137.0,28.0,20.0,16.0,35.0,46.0,71.0,0.0,492.0,0.0,71.0,70.0,153.0,55.0,0.0,34.0,203.0,214.0,1683.0,261.0,98.0,1.0,77.0,54.0,0.0,9.0,576.0,158.0,0.0,175.0,172.0,42.0,16.0,267.0,0.0,39.0,0.0,0.0,53.0,1559.0,0.0,17.0,184.0,95.0,546.0,457.0,7.0,270.0,743.0,120.0,81.0,134.0,24.0,98.0,0.0,59.0,33.0,15.0,2.0,0.0,94.0,22.0,41.0,157.0,11.0,81.0,97.0,63.0,119.0,251.0,639.0,236.0,137.0,101.0,715.0,902.0,322.0,111.0,144.0,965.0,559.0,906.0,102.0,66.0,5.0,0.0,120.0,4.0,224.0,788.0,187.0,141.0,97.0,242.0,21.0,253.0,64.0,7.0,0.0,0.0,74.0,228.0,268.0,40.0,23.0,242.0,751.0,7.0,0.0,51.0,37.0,0.0,87.0,8.0,180.0,38.0,177.0,0.0,29.0,178.0,78.0,399.0,16.0,9.0,12.0,208.0,1275.0,8.0,131.0,134.0,0.0,767.0,132.0,797.0,447.0,230.0,6.0,349.0,0.0,173.0,1044.0,1263.0,90.0,68.0,25.0,0.0,137.0,138.0,123.0,18.0,5.0,86.0,58.0,103.0,294.0,258.0,22.0,670.0,108.0,72.0,39.0,1038.0,709.0,41.0,0.0,62.0,247.0,302.0,196.0,11.0,159.0,357.0,212.0,155.0,48.0,55.0,137.0,52.0,283.0,178.0,38.0,263.0,91.0,5.0,733.0,415.0,1.0,2.0,240.0,160.0,784.0,502.0,240.0,9.0,0.0,539.0,0.0,60.0,0.0,71.0,0.0,16.0,48.0,0.0,67.0,0.0,47.0,350.0,128.0,89.0,98.0,0.0,497.0,72.0,0.0,178.0,4.0,6.0,51.0,52.0,67.0,105.0,516.0,171.0,88.0,0.0,1091.0,190.0,0.0,42.0,79.0,86.0,32.0,3.0,52.0,126.0,152.0,32.0,5.0,42.0,194.0,127.0,50.0,82.0,984.0,373.0,0.0,364.0,531.0,81.0,54.0,17.0,15.0,359.0,14.0,138.0,62.0,73.0,8.0,30.0,193.0,374.0,231.0,433.0,417.0,24.0,0.0)



  def lineBuilder(out: DataFrame, str: Array[Int], name: String): Frame = {

    val normalize = new Array[Double](normtst.length)

     for (i <-0 until normtst.length) {
       var m = math.max(normtra(i),normtst(i))
       if (m>3) m = m/3
       normalize(i) = m
     }

      println("val NORMALIZE=Array("+ normalize+")" )

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
                  chunks(i).addNum(ae.getByte(i) / normalize(i-6))
                } catch {
                  case ss: ClassCastException => {
                    try {
                      chunks(i).addNum(ae.getShort(i) / normalize(i-6))
                    } catch {
                      case bs: ClassCastException => {
                        chunks(i).addNum(ae.getInt(i) / normalize(i-6))
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


}
