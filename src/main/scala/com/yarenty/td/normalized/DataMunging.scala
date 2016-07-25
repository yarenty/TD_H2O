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
object DataMunging extends SparkContextSupport {


  val input_data_dir = "/opt/data/TalkingData/input/"
  val gender_age_train = "gender_age_train.csv"
  val app_events = "app_events.csv"
  val events = "events.csv"
  val app_labels = "app_labels.csv"
  val label_categories = "label_categories.csv"
  val phone_brand = "phone_brand_device_model.csv"

  val output_filename = "/opt/data/TalkingData/model/train"


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)


    addFiles(h2oContext.sparkContext,
      absPath(input_data_dir + gender_age_train),
      absPath(input_data_dir + phone_brand),
      absPath(input_data_dir + app_events),
      absPath(input_data_dir + app_labels),
      absPath(input_data_dir + label_categories),
      absPath(input_data_dir + events)
    )



    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



    val genderAgeData = new h2o.H2OFrame(GenderAgeCSVParser.get, new File(SparkFiles.get(gender_age_train)))
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



    ///get list of apptypes for each device_id
    val ap = sqlContext.sql(" select distinct label_id, device_id from labels, apps, events " +
      " where labels.app_id = apps.app_id and apps.event_id = events.event_id ")
    ap.registerTempTable("apptypes")
    val o = sqlContext.sql("select g.device_id as device_id, gender, age, phone_brand, device_model, grup, label_id " +
      " from " +
      "  (select genderage.device_id as device_id, " +
      "     first(gender) as gender, first(age) as age, first(group) as grup, " +
      "     first(phone_brand) as phone_brand, first(device_model) as device_model " +
      "     from genderage , phones  where genderage.device_id = phones.device_id group by genderage.device_id) as g" +
      "  LEFT JOIN  apptypes ON g.device_id = apptypes.device_id ")

    val out = o.groupBy("device_id", "gender", "age", "phone_brand", "device_model", "grup").agg(
      GroupConcat(o("label_id")).alias("labels")).distinct()

    out.take(20).foreach(println)


    for (m <- sc.getExecutorMemoryStatus) println("MEMORY STATUS:: " + m._1 + " => " + m._2)



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


  //Dropping constant columns: [907, 908, 909, 912, 913, 914, 915, 916, 1, 2, 4, 6, 800, 921,
  // 922, 925, 930, 1005, 1002, 700, 1001, 701, 702, 703, 704, 831, 832, 833, 834, 835, 836,
  // 837, 838, 839, 962, 600, 963, 601, 964, 602, 965, 603, 966, 604, 967, 605, 726, 968, 606,
  // 727, 607, 728, 849, 608, 729, 609, 850, 730, 851, 610, 852, 611, 853, 612, 854, 613, 734, 614, 615,
  // 736, 616, 617, 618, 619, 980, 740, 982, 620, 741, 500, 621, 742, 501, 622, 743, 502, 623, 744, 986,
  // 503, 624, 504, 625, 746, 505, 626, 747, 506, 627, 507, 628, 508, 629, 509, 990, 630, 751, 993, 510, 631,
  // 511, 632, 995, 512, 633, 754, 513, 634, 755, 876, 514, 635, 877, 515, 636, 878, 516, 637, 879, 517, 638, 518,
  // 639, 519, 880, 881, 640, 882, 520, 641, 883, 400, 521, 642, 884, 401, 522, 643, 764, 885, 402, 523, 644, 765,
  // 886, 403, 524, 645, 766, 887, 404, 525, 646, 767, 888, 405, 526, 647, 768, 889, 527, 648, 769, 528, 649, 529,
  // 409, 890, 770, 891, 650, 892, 530, 651, 893, 410, 531, 652, 894, 411, 532, 653, 895, 412, 533, 654, 896, 413, 534,
  // 655, 897, 414, 535, 656, 898, 415, 536, 657, 899, 416, 537, 658, 417, 538, 659, 418, 539, 419, 660, 540, 661, 420, 541,
  // 662, 300, 421, 542, 663, 301, 422, 543, 664, 785, 302, 423, 544, 665, 424, 545, 666, 425, 546, 667, 305, 426, 547,
  // 668, 306, 427, 548, 669, 428, 308, 429, 309, 670, 671, 430, 551, 672, 310, 431, 673, 311, 432, 674, 312, 433, 675,
  // 313, 434, 555, 676, 314, 435, 677, 315, 436, 557, 678, 316, 437, 558, 679, 438, 439, 680, 681, 440, 561, 682, 320, 441,
  // 562, 683, 321, 442, 684, 322, 443, 685, 323, 444, 686, 203, 324, 445, 687, 325, 446, 688, 326, 447, 568, 448, 328, 449, 329,
  // 209, 570, 450, 571, 330, 451, 572, 331, 452, 573, 694, 332, 453, 574, 333, 454, 575, 334, 455, 576, 697, 335, 456, 577, 698,
  // 336, 457, 578, 699, 337, 458, 579, 338, 459, 339, 580, 460, 581, 340, 461, 582, 341, 462, 583, 342, 463, 584, 343, 464, 585,
  // 344, 465, 586, 345, 466, 587, 346, 467, 588, 347, 468, 589, 348, 469, 107, 349, 108, 229, 109, 590, 470, 591, 350, 471, 592,
  // 351, 472, 593, 110, 352, 473, 594, 111, 353, 474, 595, 112, 354, 475, 596, 113, 355, 476, 597, 114, 356, 477, 598, 115, 357,
  // 478, 599, 116, 358, 479, 117, 359, 118, 119, 16, 480, 360, 481, 240, 361, 482, 120, 362, 483, 121, 363, 484, 122, 243, 364,
  // 485, 123, 244, 365, 486, 124, 245, 366, 487, 125, 367, 488, 126, 368, 489, 127, 369, 128, 249, 29, 490, 370, 491, 371, 492,
  // 372, 493, 373, 494, 374, 495, 375, 496, 134, 376, 497, 377, 498, 378, 499, 379, 35, 380, 381, 382, 383, 384, 385, 144, 265,
  // 386, 266, 387, 146, 388, 147, 389, 390, 391, 392, 393, 394, 395, 396, 397, 156, 398, 399, 279, 286, 166, 287, 288, 289, 68,
  // 290, 291, 292, 172, 293, 294, 295, 296, 297, 177, 298, 299, 183, 197, 900, 901, 902, 903, 904, 905, 906]


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

      //println("AND::" + ae.get(0)+ "::"+ae.get(1) +"::"+ae.get(2))
      chunks(0).addStr(ae.getString(0)) //device_id
      val gender: Int = if (ae.getString(1).equals("F")) 0 else 1
      chunks(1).addNum(gender)
      chunks(2).addNum(ae.getString(2).toInt) //age
      chunks(3).addStr(ae.getString(3))
      chunks(4).addStr(ae.getString(4))
      chunks(5).addStr(ae.getString(5))

      if (ae.get(startIdx) != null && ae.getString(startIdx).length > 2) {
        val m = Helper.mapCreator(ae.getString(startIdx))
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

    val key = Key.make("Apps")
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
