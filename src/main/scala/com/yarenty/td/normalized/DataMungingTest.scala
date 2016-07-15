package com.yarenty.td.normalized

import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.h2o._
import water._
import water.fvec._

import scala.collection.mutable
import scala.collection.mutable.HashMap

//import org.apache.spark.{SparkFiles, h2o, SparkContext}
//import org.apache.spark.h2o.{RDD, H2OFrame, DoubleHolder, H2OContext}
import com.yarenty.td.schemas._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import water.support.SparkContextSupport


/**
  * Created by yarenty on 15/07/2016.
  * (C)2015 SkyCorp Ltd.
  */
object DataMungingTest extends SparkContextSupport {


  def process(h2oContext: H2OContext) {

    import h2oContext._
    import h2oContext.implicits._
    val sc = h2oContext.sparkContext
    implicit val sqlContext = new SQLContext(sc)

  }


}
