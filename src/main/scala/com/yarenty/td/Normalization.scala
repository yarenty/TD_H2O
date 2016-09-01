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
object Normalization extends SparkContextSupport {


  val data_dir = "/opt/data/TalkingData/model/"

  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)



    println(s"\n\n LETS MODEL\n")



    addFiles(sc, absPath(data_dir + "test"))
    addFiles(sc, absPath(data_dir + "train"))
    val trainURI = new URI("file:///" + SparkFiles.get("train"))
    val testURI = new URI("file:///" + SparkFiles.get("test"))

    val trainData = new h2o.H2OFrame(ModelCSVParser.get, trainURI)
    val testData = new h2o.H2OFrame(ModelCSVParser.get, testURI)


    val startIdx = 6
    val normal = new Array[Double](1022)
    for (i <- 1 to 1021) {
      normal(i) = trainData.vec(i.toString).max()
    }

    print("(")
    for(d <- normal)
      {
        print(d + ",")
      }
    println(")")


    val diff = new Array[Double](1022)
    for (i <- 1 to 1021) {
      diff(i) = trainData.vec(i.toString).mean()
    }

    print("(")
    for(d <- diff)
      {
        print(d + ",")
      }
    println(")")

//
//    //remove unused data
//    val toRemove = Array(907, 908, 911, 912, 913, 914, 915, 1, 3, 5, 920, 921, 924, 929, 1000, 1004, 700, 1001, 701, 702, 703, 830, 831, 832, 833, 834, 835, 836, 837, 838, 961, 962, 600, 963, 601, 964, 602, 965, 603, 966, 604, 725, 967, 605, 726, 606, 727, 848, 607, 728, 849, 608, 729, 609, 850, 851, 610, 852, 611, 853, 612, 733, 613, 614, 735, 615, 616, 979, 617, 618, 739, 619, 981, 740, 620, 741, 500, 621, 742, 501, 622, 743, 985, 502, 623, 503, 624, 745, 504, 625, 746, 505, 626, 989, 506, 627, 507, 628, 508, 629, 509, 750, 992, 630, 510, 631, 994, 511, 632, 753, 512, 633, 754, 875, 513, 634, 876, 514, 635, 877, 515, 636, 878, 516, 637, 879, 517, 638, 518, 639, 519, 880, 881, 640, 882, 520, 641, 883, 400, 521, 642, 763, 884, 401, 522, 643, 764, 885, 402, 523, 644, 765, 886, 403, 524, 645, 766, 887, 404, 525, 646, 767, 888, 526, 647, 768, 889, 527, 648, 769, 528, 649, 408, 529, 409, 890, 891, 650, 892, 530, 651, 893, 410, 531, 652, 894, 411, 532, 653, 895, 412, 533, 654, 896, 413, 534, 655, 897, 414, 535, 656, 898, 415, 536, 657, 899, 416, 537, 658, 417, 538, 659, 418, 539, 419, 660, 540, 661, 420, 541, 662, 300, 421, 542, 663, 784, 301, 422, 543, 664, 423, 544, 665, 424, 545, 666, 304, 425, 546, 667, 305, 426, 547, 668, 427, 669, 307, 428, 308, 429, 309, 670, 550, 671, 430, 672, 310, 431, 673, 311, 432, 674, 312, 433, 554, 675, 313, 434, 676, 314, 435, 556, 677, 315, 436, 557, 678, 799, 437, 679, 438, 439, 319, 680, 560, 681, 440, 561, 682, 320, 441, 683, 321, 442, 684, 322, 443, 685, 202, 323, 444, 686, 324, 445, 687, 325, 446, 567, 447, 327, 448, 569, 328, 449, 208, 329, 570, 450, 571, 330, 451, 572, 693, 331, 452, 573, 332, 453, 574, 333, 454, 575, 696, 334, 455, 576, 697, 335, 456, 577, 698, 336, 457, 578, 699, 337, 458, 579, 338, 459, 339, 580, 460, 581, 340, 461, 582, 341, 462, 583, 342, 463, 584, 343, 464, 585, 344, 465, 586, 345, 466, 587, 346, 467, 588, 347, 468, 589, 106, 348, 469, 107, 228, 349, 108, 109, 590, 470, 591, 350, 471, 592, 351, 472, 593, 110, 352, 473, 594, 111, 353, 474, 595, 112, 354, 475, 596, 113, 355, 476, 597, 114, 356, 477, 598, 115, 357, 478, 599, 116, 358, 479, 117, 359, 118, 239, 119, 15, 480, 360, 481, 361, 482, 120, 362, 483, 121, 363, 484, 122, 243, 364, 485, 123, 244, 365, 486, 124, 366, 487, 125, 367, 488, 126, 368, 489, 127, 248, 369, 28, 490, 370, 491, 371, 492, 372, 493, 373, 494, 374, 495, 133, 375, 496, 376, 497, 377, 498, 378, 499, 379, 34, 380, 381, 382, 383, 384, 143, 264, 385, 386, 145, 387, 146, 388, 389, 390, 391, 392, 393, 394, 395, 396, 155, 397, 398, 278, 399, 285, 165, 286, 287, 288, 289, 67, 290, 291, 171, 292, 293, 294, 295, 296, 176, 297, 298, 299, 182, 196, 900, 901, 902, 903, 904, 905, 906)
//    for (i <- toRemove) {
//      trainData.remove(i.toString)
//      testData.remove(i.toString)
//    }
//

  }


}