package com.yarenty.td

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.URI

import com.yarenty.td.schemas._
import com.yarenty.td.utils.Helper
import hex.Distribution
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.naivebayes.{NaiveBayes, NaiveBayesModel}
import hex.naivebayes.NaiveBayesModel.NaiveBayesParameters
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



  val data_dir = "/opt/data/TalkingData/model/"

  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    println(s"\n\n LETS MODEL\n")



    addFiles(sc, absPath(data_dir + "test"))
    addFiles(sc, absPath(data_dir + "train"))
    val trainURI= new URI("file:///" + SparkFiles.get("train" ))
    val testURI= new URI("file:///" + SparkFiles.get("test" ))

    val trainData = new h2o.H2OFrame(ModelCSVParser.get, trainURI)
    val testData = new h2o.H2OFrame(ModelCSVParser.get, testURI)




    val nbModel = DRFModel(trainData)

    // SAVE THE MODEL!!!
//    val om = new FileOutputStream(data_dir +"DRFModel_" + System.currentTimeMillis() + ".java")
//    nbModel.toJava(om, false, false)
//    val omab = new FileOutputStream(data_dir + "/DRFModel_" + System.currentTimeMillis() + ".hex")
//    val ab = new AutoBuffer(omab, true)
//    nbModel.write(ab)
//    ab.close()
//    println("JAVA and hex(iced) models saved.")

    val predict = nbModel.score(testData)

//    predict.add("device_id", testData.vec("device"))
//    predict.remove("predict")
    predict.rename("predict","device_id")
    predict.rename(0,"device_id")
    predict.replace(0,testData.vec("device"))
    // testData.add(predict)
      val out = new H2OFrame(predict)

    Helper.saveCSV(out,data_dir + "submit.csv")

    println("=========> off to go!!!")
  }




  /** ****************************************************
    * MODELS
    * *****************************************************/

  //buildModel 'naivebayes',
  // {"model_id":"naivebayes-cc672e2a-04f3-475d-8738-5bcee62c395e",
  // "nfolds":0,"training_frame":"train.hex",
  // "response_column":"grup",
  // "ignored_columns":["device","gender","age"],"ignore_const_cols":true,
  // "laplace":0,"min_sdev":0.001,"eps_sdev":0,"min_prob":0.001,"eps_prob":0,
  // "compute_metrics":true,"score_each_iteration":false,
  // "max_confusion_matrix_size":20,"max_hit_ratio_k":0,"max_runtime_secs":0,"seed":0}

  def NBModel(smOutputTrain: H2OFrame): NaiveBayesModel = {     //logloss = 3.65712

    val params = new NaiveBayesParameters()
    params._train = smOutputTrain.key
    params._distribution = Distribution.Family.gaussian
    params._response_column = "grup"
    params._ignored_columns = Array("device","gender","age")
    params._ignore_const_cols = true


    println("BUILDING:" + params.fullName)
    val dl = new NaiveBayes(params)
    dl.trainModel.get
  }



//  buildModel 'drf',
//  {"model_id":"drf-01109063-2a3f-4478-8ef1-971197114a64",
//    "training_frame":"train.hex",
//    "nfolds":0,
//    "response_column":"grup",
//    "ignored_columns":["device","gender","age"],
//    "ignore_const_cols":true,"ntrees":50,
//    "max_depth":20,"min_rows":1,"nbins":20,"seed":-1,"mtries":-1,
//    "sample_rate":0.6320000290870667,"score_each_iteration":false,
//    "score_tree_interval":0,"balance_classes":false,"max_confusion_matrix_size":20,"max_hit_ratio_k":0,
//    "nbins_top_level":1024,"nbins_cats":1024,"r2_stopping":0.999999,"stopping_rounds":0,"stopping_metric":"AUTO",
//    "stopping_tolerance":0.001,"max_runtime_secs":0,"checkpoint":"","col_sample_rate_per_tree":1,
//    "min_split_improvement":0,"histogram_type":"AUTO","build_tree_one_node":false,
//    "sample_rate_per_class":[],"binomial_double_trees":false,"col_sample_rate_change_per_level":1}

  //seed -1188814820856564594

  def DRFModel(smOutputTrain: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
//    params._distribution = Distribution.Family.gaussian
    params._response_column = "grup"
    params._ignored_columns = Array("device","gender","age")
    params._ignore_const_cols = true
    params._seed = -1188814820856564594L


    println("BUILDING:" + params.fullName)
    val dl = new DRF(params)
    dl.trainModel.get
  }
}

