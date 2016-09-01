package com.yarenty.td

import java.net.URI

import com.yarenty.td.schemas._
import com.yarenty.td.utils.Helper
import hex.Distribution
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.{DeepLearning, DeepLearningModel}
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
object BuildAdvancedAllAgeGenderModel extends SparkContextSupport {


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
    try {

      addFiles(sc, absPath(data_dir + "all_train"))
      val trainURI = new URI("file:///" + SparkFiles.get("all_train"))
      val tData = new h2o.H2OFrame(ModelFullCSVParser.get, trainURI)




      println("SPLIT")
      val dt = asDataFrame(tData).randomSplit(Array(0.9, 0.1), 999)

      trainData = asH2OFrame(dt(0), "train")
      validData = asH2OFrame(dt(1), "valid")

      trainData.colToEnum(Array("gender", "brand", "model", "grup"))
      validData.colToEnum(Array("gender", "brand", "model", "grup"))

      Helper.saveCSV(trainData, data_dir + "train_09")
      Helper.saveCSV(validData, data_dir + "valid_01")

    } catch {
      case e: Exception => //do nothing
    }

    //FIXME:



//    //JUST PLAING:
//        addFiles(sc, absPath(data_dir + "train_09"))
//        trainData = new h2o.H2OFrame(ModelFullCSVParser.get, new URI("file:///" + SparkFiles.get("train_09" )) )
//        addFiles(sc, absPath(data_dir + "valid_01"))
//        validData = new h2o.H2OFrame(ModelFullCSVParser.get, new URI("file:///" + SparkFiles.get("valid_01" )))
//
//        trainData.colToEnum(Array("gender", "brand", "model", "grup"))
//        validData.colToEnum(Array("gender", "brand", "model", "grup"))



    println("MODEL")
    val ageModel = DLAgeModel(trainData, validData)
    val genderModel = DLGenderModel(trainData, validData)



    val ageTrain = ageModel.score(trainData)
    println("Train AGE is there")
    val genderTrain = genderModel.score(trainData)
    println("Train Gender is there")
    trainData.remove("age")
    trainData.add("age", ageTrain.vec("predict"))
    trainData.remove("gender")
    trainData.add("gender",genderTrain.vec("predict"))


    val ageValid = ageModel.score(validData)
    println("Train AGE is there")
    val genderValid = genderModel.score(validData)
    println("Train Gender is there")
    validData.remove("age")
    validData.add("age", ageValid.vec("predict"))
    validData.remove("gender")
    validData.add("gender",genderValid.vec("predict"))


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

    val age = ageModel.score(testData)
    println("AGE is there")

    val gender = genderModel.score(testData)
    println("Gender is there")


    testData.remove("age")
    testData.add("age", age.vec("predict"))


    testData.remove("gender")
    testData.add("gender",gender.vec("predict"))


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


 //2.3537  => 2.29 /2.33    I need  2.10/ 2.20

  def DLModel(train: H2OFrame, valid: H2OFrame): DeepLearningModel = {


    val params = new DeepLearningParameters
    params._train = train.key
    params._valid = valid.key
    params._response_column = "grup"
    params._ignored_columns = Array("device")
    params._ignore_const_cols = true
    params._epochs = 500.0
    params._standardize = true
//    params._score_each_iteration = true
    //    params._variable_importances = true

    params._seed = -6760352068770269000L    //2.2663-2.3119
//    params._overwrite_with_best_model = false

//    params._loss = DeepLearningParameters.Loss.CrossEntropy
//    params._distribution = Distribution.Family.multinomial
    //params._shuffle_training_data = true

     params._variable_importances= true
    //params._missing_values_handling =   MissingValuesHandling.Skip

//    params._hidden = Array( 545,109,36)
//    params._hidden = Array(512,256,128,64) //Feel Lucky
//    params._hidden = Array(200,200,200) //Feel Lucky
    // params._hidden = Array(512) //Eagle Eye
    // params._hidden = Array(64, 64, 64) //Puppy Brain
     params._hidden = Array(32,32,32,32,32) //Junior Chess Master
    println("BUILDING:" + params.fullName)
    val dl = new DeepLearning(params)
    dl.trainModel.get
  }


  def DLAgeModel(train: H2OFrame, valid: H2OFrame): DeepLearningModel = {
      val params = new DeepLearningParameters
      params._train = train.key
      params._valid = valid.key
      params._response_column = "age"
      params._ignored_columns = Array("device", "gender", "grup")
      params._ignore_const_cols = true
      params._epochs = 500.0
      params._standardize = true

      //    params._seed = -8823609696683622000L // 6433149976926940000L    //-8996666368897430268
  //    params._hidden = Array( 545,109,36)
  //    params._hidden = Array(512,256,128,64) //Feel Lucky
  //    params._hidden = Array(200,200,200) //Feel Lucky
      // params._hidden = Array(512) //Eagle Eye
       params._hidden = Array(64, 64, 64) //Puppy Brain
  //     params._hidden = Array(32,32,32,32,32) //Junior Chess Master

      println("BUILDING:" + params.fullName)
      val dl = new DeepLearning(params)
      dl.trainModel.get
    }


  def DLGenderModel(train: H2OFrame, valid: H2OFrame): DeepLearningModel = {
      val params = new DeepLearningParameters
      params._train = train.key
      params._valid = valid.key
      params._response_column = "gender"
      params._ignored_columns = Array("device", "age", "grup")
      params._ignore_const_cols = true
      params._epochs = 500.0
      params._standardize = true
      //    params._seed = -8823609696683622000L // 6433149976926940000L    //-8996666368897430268
  //    params._hidden = Array( 545,109,36)
  //    params._hidden = Array(512,256,128,64) //Feel Lucky
  //    params._hidden = Array(200,200,200) //Feel Lucky
      // params._hidden = Array(512) //Eagle Eye
       params._hidden = Array(64, 64, 64) //Puppy Brain
  //     params._hidden = Array(32,32,32,32,32) //Junior Chess Master

      println("BUILDING:" + params.fullName)
      val dl = new DeepLearning(params)
      dl.trainModel.get
    }

}

