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


    var trainData: H2OFrame = null
    var validData: H2OFrame = null

    addFiles(sc, absPath(data_dir + "full_train"))
    val trainURI = new URI("file:///" + SparkFiles.get("full_train"))
    val tData = new h2o.H2OFrame(ModelFullCSVParser.get, trainURI)




    println("SPLIT")
    val dt = asDataFrame(tData).randomSplit(Array(0.9, 0.1), 666)

    trainData = asH2OFrame(dt(0), "train")
    validData = asH2OFrame(dt(1), "valid")

    trainData.colToEnum(Array("gender", "brand", "model", "grup"))
    validData.colToEnum(Array("gender", "brand", "model", "grup"))

    println("MODEL")
    val nbModel = DLModel(trainData, validData)

    println("DONE")
    // SAVE THE MODEL!!!
    //    val om = new FileOutputStream(data_dir +"DRFModel_" + System.currentTimeMillis() + ".java")
    //    nbModel.toJava(om, false, false)
    //    val omab = new FileOutputStream(data_dir + "/DRFModel_" + System.currentTimeMillis() + ".hex")
    //    val ab = new AutoBuffer(omab, true)
    //    nbModel.write(ab)
    //    ab.close()
    //    println("JAVA and hex(iced) models saved.")


    addFiles(sc, absPath(data_dir + "full_test"))
    val testURI = new URI("file:///" + SparkFiles.get("full_test"))
    val testData = new h2o.H2OFrame(ModelFullCSVParser.get, testURI)
    testData.colToEnum(Array("brand", "model"))

    println("TEST")
    val predict = nbModel.score(testData)

    //    predict.add("device_id", testData.vec("device"))
    //    predict.remove("predict")
    println("RENAMING - FIX")
    predict.replace(0, testData.vec("device"))
    predict.rename("predict", "device_id")
    predict.rename(0, "device_id")

    // testData.add(predict)

    println("OUT")
    val out = new H2OFrame(predict)

    Helper.saveCSV(out, data_dir + "submit_full.csv")

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

  def NBModel(train: H2OFrame, valid: H2OFrame): NaiveBayesModel = {
    //logloss = 3.65712

    val params = new NaiveBayesParameters()
    params._train = train.key
    params._valid = valid.key
    params._distribution = Distribution.Family.gaussian
    params._response_column = "grup"
    params._ignored_columns = Array("device", "gender", "age")
    params._ignore_const_cols = true


    println("BUILDING:" + params.fullName)
    val dl = new NaiveBayes(params)
    dl.trainModel.get
  }


  // 2.2323
  //buildModel 'drf', {"model_id":"drf_better3","training_frame":"train","validation_frame":"valid",
  //    "nfolds":0,"response_column":"grup","ignored_columns":["device","gender","age"],
  //    "ignore_const_cols":true,"ntrees":"100","max_depth":"20","min_rows":1,"nbins":20,"seed":-1,
  //    "mtries":-1,"sample_rate":0.6320000290870667,"score_each_iteration":false,"score_tree_interval":0,
  //    "balance_classes":false,"max_confusion_matrix_size":20,"max_hit_ratio_k":0,"nbins_top_level":1024,
  //    "nbins_cats":1024,"r2_stopping":0.999999,"stopping_rounds":0,"stopping_metric":"AUTO",
  //    "stopping_tolerance":0.001,"max_runtime_secs":0,"checkpoint":"","col_sample_rate_per_tree":1,
  //    "min_split_improvement":0,"histogram_type":"AUTO","build_tree_one_node":false,"sample_rate_per_class":[],
  //    "binomial_double_trees":false,"col_sample_rate_change_per_level":1}

  //seed -1188814820856564594    =  2.34860

  def DRFModel(smOutputTrain: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = smOutputTrain.key
    //    params._distribution = Distribution.Family.gaussian
    params._response_column = "grup"
    params._ignored_columns = Array("device", "gender", "age")
    params._ignore_const_cols = true
    //    params._seed =  -6242730077026816667 //-1188814820856564594L  //5428260616053944984


    params._ntrees = 500
    params._max_depth = 20 //-2515230053271016359
    params._distribution = Distribution.Family.AUTO


    println("BUILDING:" + params.fullName)
    val dl = new DRF(params)
    dl.trainModel.get
  }


  //buildModel 'deeplearning', {"model_id":"deeplearning-d2cbde4d-79d0-407a-bc23-2a2777c9c942",
  // "training_frame":"train.hex","nfolds":0,"response_column":"grup",
  // "ignored_columns":["device","gender","age"],"ignore_const_cols":true,
  // "activation":"Rectifier","hidden":[200,200],"epochs":10,
  // "variable_importances":false,"score_each_iteration":false,
  // "balance_classes":false,"max_confusion_matrix_size":20,
  // "max_hit_ratio_k":0,"checkpoint":"","use_all_factor_levels":true,
  // "standardize":true,"train_samples_per_iteration":-2,
  // "adaptive_rate":true,"input_dropout_ratio":0,
  // "l1":0,"l2":0,"loss":"Automatic","distribution":"AUTO",
  // "score_interval":5,"score_training_samples":10000,
  // "score_duty_cycle":0.1,"stopping_rounds":5,"stopping_metric":"AUTO",
  // "stopping_tolerance":0,"max_runtime_secs":0,"autoencoder":false,"pretrained_autoencoder":"",
  // "overwrite_with_best_model":true,"target_ratio_comm_to_comp":0.05,
  // "seed":-8823609696683622000,
  // "rho":0.99,"epsilon":1e-8,"max_w2":"Infinity","initial_weight_distribution":"UniformAdaptive",
  // "classification_stop":0,"diagnostics":true,"fast_mode":true,"force_load_balance":true,"single_node_mode":false,
  // "shuffle_training_data":false,"missing_values_handling":"MeanImputation","quiet_mode":false,"sparse":false,
  // "col_major":false,"average_activation":0,"sparsity_beta":0,"max_categorical_features":2147483647,
  // "reproducible":false,"export_weights_and_biases":false,"mini_batch_size":1,"elastic_averaging":false}


  //  model_checksum	-8135066134514726912
  //  frame	·
  //  frame_checksum	0
  //  description	Metrics reported on temporary training frame with 9960 samples
  //  model_category	Multinomial
  //  scoring_time	1469548286004
  //  predictions	·
  //  MSE	0.664070
  //  r2	0.942583
  //  logloss	1.987232


  def DLModel(train: H2OFrame, valid: H2OFrame): DeepLearningModel = {


    val params = new DeepLearningParameters
    params._train = train.key
    params._valid = valid.key
    params._response_column = "grup"
    params._ignored_columns = Array("device", "gender", "age")
    params._ignore_const_cols = true

    //    params._seed = -8823609696683622000L // 6433149976926940000L    //-8996666368897430268

    //    params._hidden = Array(512,256,128,64) //Feel Lucky
    params._hidden = Array(288, 144, 72, 36) //Feel Lucky
    //    params._hidden = Array(200,200) //Feel Lucky
    // params._hidden = Array(512) //Eagle Eye
    // params._hidden = Array(64,64,64) //Puppy Brain
    // params._hidden = Array(32,32,32,32,32) //Junior Chess Master



    params._epochs = 200.0
    params._standardize = true
    //    params._score_each_iteration = true
    //    params._variable_importances = true


    println("BUILDING:" + params.fullName)
    val dl = new DeepLearning(params)
    dl.trainModel.get
  }

}

