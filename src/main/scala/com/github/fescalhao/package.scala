package com.github

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
import scala.io.Source

package object SparkPackageUtils {
  def getSparkContext(appName: String): SparkContext = {
    val conf = getSparkConf(appName)

    new SparkContext(config = conf)
  }

  def getSparkSession(appName: String): SparkSession = {
    val conf = getSparkConf(appName)

    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  private def getSparkConf(appName: String): SparkConf = {
    val sparkConf = new SparkConf()
    val props = getSparkConfProperties
    props.forEach((k,v) => {
      sparkConf.set(k.toString, v.toString)
    })

    sparkConf.setAppName(appName)
  }

  private def getSparkConfProperties: Properties = {
    val props = new Properties()
    props.load(Source.fromFile("spark.conf").bufferedReader())

    props
  }
}
