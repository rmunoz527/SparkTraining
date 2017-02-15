package com.trace3.sparktraining
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
/**
 * Hello world!
 *
 */
object LogFilter extends App {
  val sconf = new SparkConf().setAppName("Log Analyzer")
                            .setMaster("local[2]")
                            .set("spark.executor.memory","1g")
  val sc = new SparkContext(sconf)
  // Reads log file on local in this example
  val weblogs = sc.textFile("file:///home/rmunoz/Projects/SparkTraining/data/weblogs/2013-09-15.log")
  val targetfile = "/home/rmunoz/Projects/SparkTraining/data/targetmodels.txt"
  val targetDevices = Source.fromFile(targetfile).getLines().toList
  // Broadcasts target file to nodes
  val bcTargetDevices = sc.broadcast(targetDevices)
  // Filters weblogs RDD to requests executed by target devices
  val targetDeviceLogs = weblogs.filter(line => bcTargetDevices.value.count(line.contains(_)) > 0)
  println(weblogs.count())
  println(targetDeviceLogs.count())
}
