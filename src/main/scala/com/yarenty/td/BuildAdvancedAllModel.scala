package com.yarenty.td

import java.net.URI
import java.util

import com.yarenty.td.normalized.EventsClusteringProcessing
import com.yarenty.td.schemas._
import com.yarenty.td.utils.Helper
import hex.Distribution
import hex.ScoreKeeper.StoppingMetric
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters.MissingValuesHandling
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import hex.grid.{Grid, GridSearch}
import hex.naivebayes.NaiveBayesModel.NaiveBayesParameters
import hex.naivebayes.{NaiveBayes, NaiveBayesModel}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.drf.{DRF, DRFModel}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkFiles, h2o}
import water.support.SparkContextSupport

/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object BuildAdvancedAllModel extends SparkContextSupport {


  val data_dir = "/opt/data/TalkingData/model/"

  def process(h2oContext: H2OContext) {

    val sc = h2oContext.sparkContext

    import h2oContext._
    import h2oContext.implicits._
    implicit val sqlContext = new SQLContext(sc)


    println(s"\n\n LETS MODEL\n")


    var trainData: H2OFrame = null
    var validData: H2OFrame = null

    //RECREATE::

//      addFiles(sc, absPath(data_dir + "all_train"))
//      val trainURI = new URI("file:///" + SparkFiles.get("all_train"))
//      val tData = new h2o.H2OFrame(ModelFullCSVParser.get, trainURI)
//
//
//
//
//
//      println("SPLIT")
//      val dt = asDataFrame(tData).randomSplit(Array(0.99, 0.01), 666)
//
//      trainData = asH2OFrame(dt(0), "train")
//      validData = asH2OFrame(dt(1), "valid")
//
//      trainData.colToEnum(Array("gender", "brand", "model", "grup"))
//      validData.colToEnum(Array("gender", "brand", "model", "grup"))
//
//      Helper.saveCSV(trainData, data_dir + "train_09")
//      Helper.saveCSV(validData, data_dir + "valid_01")



    //FIXME:
    //JUST PLAING:
        addFiles(sc, absPath(data_dir + "train_09"))
        trainData = new h2o.H2OFrame(ModelFullCSVParser.get, new URI("file:///" + SparkFiles.get("train_09" )) )


        addFiles(sc, absPath(data_dir + "valid_01"))
        validData = new h2o.H2OFrame(ModelFullCSVParser.get, new URI("file:///" + SparkFiles.get("valid_01" )))

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


    addFiles(sc, absPath(data_dir + "all_test"))
    val testURI = new URI("file:///" + SparkFiles.get("all_test"))
    val testData = new h2o.H2OFrame(ModelFullCSVParser.get, testURI)
    testData.colToEnum(Array("brand", "model"))


    println("TEST")
    val predict = nbModel.score(testData)


    //    predict.add("device_id", testData.vec("device"))
    //    predict.remove("predict")
    println("RENAMING - FIX")
    predict.add("device_id",testData.vec("device"))
    predict.remove("predict")


    // testData.add(predict)

    println("OUT")
    val out = new H2OFrame(predict)

    Helper.saveCSV(out, data_dir + "submit_all.csv")

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

  def DRFModel(train: H2OFrame, valid: H2OFrame): DRFModel = {

    val params = new DRFParameters()
    params._train = train.key
    params._valid = valid.key
    //    params._distribution = Distribution.Family.gaussian
    params._response_column = "grup"
    params._ignored_columns = Array("device", "gender", "age") ++ (0 until EventsClusteringProcessing.K).map(i => "c"+i.toString)
    params._ignore_const_cols = true
    //    params._seed =  -6242730077026816667 //-1188814820856564594L  //5428260616053944984

    params._nbins = 256

    params._ntrees = 1500
    params._max_depth = 40 //-2515230053271016359
    params._distribution = Distribution.Family.AUTO


    println("BUILDING:" + params.fullName)
    val dl = new DRF(params)
    dl.trainModel.get
  }


 //2.3537  => 2.29 /2.33    I need  2.10/ 2.20

  def DLModel(train: H2OFrame, valid: H2OFrame): DeepLearningModel = {


    val params = new DeepLearningParameters
    params._train = train.key
    params._valid = valid.key
    params._response_column = "grup"
    params._ignored_columns = Array("device", "gender", "age") ++ (0 until EventsClusteringProcessing.K).map(i => "c"+i.toString)
    params._ignore_const_cols = true
    params._epochs = 500.0
//    params._standardize = false
    params._activation = DeepLearningParameters.Activation.TanhWithDropout
//    params._input_dropout_ratio =0.02
    params._hidden_dropout_ratios = Array(0.05,0.05,0.05,0.05,0.05,0.05)

//    params._score_each_iteration = true
    params._variable_importances = true

    params._max_w2  = 10
    params._stopping_metric =  StoppingMetric.logloss
    params._stopping_rounds = 42

    params._adaptive_rate = false
    params._rate = 0.01
    params._rate_annealing = 2e-6

    params._momentum_start=0.2
    params._momentum_stable=0.4
    params._momentum_ramp=1e7

    params._l1=1e-3
    params._l2=1e-3


//    params._seed = -631241300168257500L   //2.2663-2.3119
//    params._overwrite_with_best_model = false
    //params._shuffle_training_data = true
//    params._hidden = Array(768,384,192,96,48,24)
//    params._hidden = Array(512,256,128,64) //Feel Lucky
//    params._hidden = Array(200,200,200) //Feel Lucky
    // params._hidden = Array(512) //Eagle Eye
     params._hidden = Array(64,64,64,64,64,64) //Puppy Brain
//     params._hidden = Array(32,32,32,32,32,32) //Junior Chess Master
    println("BUILDING:" + params.fullName)
    val dl = new DeepLearning(params)
    dl.trainModel.get
  }

//
//  def DGrid(train: H2OFrame, valid: H2OFrame): Unit = {
//
//     // Create initial parameters and fill them by references to data
//     val params = new DeepLearningParameters
//     params._train = train.key
//     params._valid = valid.key
//      params._response_column = "grup"
//      params._ignored_columns = Array("device", "gender", "age") ++ (0 until EventsClusteringProcessing.K).map(i => "c"+i.toString)
//      params._ignore_const_cols = true
//      params._epochs = 500.0
//      params._standardize = false
//
//
//      // Define hyper-space to search
//      val hyperParms = new util.HashMap[String, Array]()
//      hyperParms.put("_ntrees", Array(1, 2))
//    hyperParms.put("_distribution",Array(Distribution.Family.multinomial))
////      hyperParms.put("_max_depth",new Integer[]{1,2,5});
////      hyperParms.put("_learn_rate",new Float[]{0.01f,0.1f,0.3f});
//
//      // Launch grid search job creating GBM models
//      val gridSearchJob = GridSearch.startGridSearch(new water.Key[Grid](),params, hyperParms);
//
//      // Block till the end of the job and get result
//      val grid = gridSearchJob.get()
//
//      // Get built models
//      val  models = grid.getModels()
//
//  }

}

