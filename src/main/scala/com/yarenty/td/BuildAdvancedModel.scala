package com.yarenty.td

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.td.schemas._
import com.yarenty.td.utils.Helper
import hex.Distribution
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.gbm.{GBM, GBMModel}
import org.apache.commons.io.FileUtils
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{h2o, SparkContext, SparkFiles}
import water.{AutoBuffer, Key}
import water.fvec.{Vec, Frame}
import water.parser.{ParseSetup, ParseDataset}
import water.support.SparkContextSupport


import MLProcessor.h2oContext._
import MLProcessor.h2oContext.implicits._
import MLProcessor.sqlContext.implicits._

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object BuildAdvancedModel extends SparkContextSupport {



  val data_dir = "/opt/data/TalkingData/model"

  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    println(s"\n\n LETS MODEL\n")


  /*
    addFiles(sc, absPath(data_dir + "test"))
    addFiles(sc, absPath(data_dir + "train"))
    val trainURI= new URI("file:///" + SparkFiles.get("train" ))
    val testURI= new URI("file:///" + SparkFiles.get("test" ))

    val trainData = new h2o.H2OFrame(OutputCSVParser.get, trainURI)
    val testData = new h2o.H2OFrame(OutputCSVParser.get, testURI)

    trainData.colToEnum(Helper.enumColumns)
    testData.colToEnum(Helper.enumColumns)

    val gapModel = drfGapModel(trainData, testData)

    // SAVE THE MODEL!!!
    val om = new FileOutputStream(data_dir +"DRFModel_" + System.currentTimeMillis() + ".java")
    gapModel.toJava(om, false, false)
    val omab = new FileOutputStream(data_dir + "/DRFModel_" + System.currentTimeMillis() + ".hex")
    val ab = new AutoBuffer(omab, true)
    gapModel.write(ab)
    ab.close()
    println("JAVA and hex(iced) models saved.")




      val predict = gapModel.score(testData)
      val vec = predict.get.lastVec

     testData.add("predict", vec)
      println("OUT VECTOR:" + vec.length)
      saveOutput(testData)
      */

    println("=========> off to go!!!")
  }


  def saveOutput(smOutputTest: H2OFrame): Unit = {

    import MLProcessor.sqlContext


    val key = Key.make("output").asInstanceOf[Key[Frame]]
    val out = new Frame(key, Helper.names, smOutputTest.vecs(Helper.names))
    val zz = new h2o.H2OFrame(out)
    val odf = asDataFrame(zz)
    odf.registerTempTable("out")

    //filtering
    val a = sqlContext.sql("select timeslice, districtID, gap, IF(predict<0.5,cast(0.0 as double), predict) as predict from out")
    a.registerTempTable("gaps")
    val o = sqlContext.sql(" select timeslice, districtID, sum(gap) as gap, sum(predict) as predict from gaps " +
      "  group by timeslice,districtID")
    o.take(20).foreach(println)
    val toSee = new H2OFrame(o)

    println(s" output should be visible now ")

    Helper.saveCSV(o,data_dir + "submit.csv")

    println(s" Ouptut  CSV created!")

  }


  /** ****************************************************
    * MODELS
    * *****************************************************/


  def drfGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key

    params._ntrees = 30 //@todo use more
    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather", "temp")
    params._ignore_const_cols = false
    params._score_each_iteration = true
    params._max_depth = 30
    params._nbins = 20
//    params._seed = 2908843745901846058L


    println("BUILDING:" + params.fullName)
    val drf = new DRF(params)
    drf.trainModel.get
  }


  def dlGapModel(smOutputTrain: H2OFrame, smOutputTest: H2OFrame): DeepLearningModel = {

    val params = new DeepLearningParameters()
    params._train = smOutputTrain.key
    params._valid = smOutputTest.key
    params._distribution = Distribution.Family.gaussian
    params._response_column = "gap"
    params._ignored_columns = Array("id", "weather")
    params._ignore_const_cols = true


    //    params._hidden = Array(200,200) //Feel Lucky   - 3.17
    //    params._hidden = Array(512) //Eagle Eye     -4.67
        params._hidden = Array(64,64,64) //Puppy Brain   -2.64
    //    params._hidden = Array(32,32,32,32,32) //Junior Chess Master
    params._mini_batch_size = 10
    params._epochs = 5.0

    println("BUILDING:" + params.fullName)
    val dl = new DeepLearning(params)
    dl.trainModel.get
  }

}

