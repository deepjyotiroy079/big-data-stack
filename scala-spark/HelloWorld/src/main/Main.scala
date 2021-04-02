/**
 * 
 * Word Count Program
 * 
 * 
 */
package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkByExample").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    val file_data = spark.sparkContext.textFile("src/main/resources/wc_data.txt")
    
    val rdd1 = file_data.flatMap(lines => lines.split(" "))
    
    val rdd2 = rdd1.map(x=>(x,1))
    val results = rdd2.reduceByKey(_+_)
    for (result <- results) {
      println(result)
    }
//    println(result.collect)
  }
}