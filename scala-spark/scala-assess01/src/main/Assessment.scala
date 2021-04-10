package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Assessment {
 
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Assessment").setMaster("local")
    val sc = new SparkContext(conf)
    
    val input_data = sc.textFile("src/resources/input.csv")
    
    val input_rdd = input_data.map({
      line => {
        val col = line.split("	")
        (col(0), col(1))
      }
    })
    
    // removing the headers
    val headers = input_rdd.first()
    
    // rdd without headers
    val input_rdd_without_headers = input_rdd.filter(line => line != headers)
//    input_rdd_without_headers.foreach(println)
    
    val id = input_rdd_without_headers.map(x=>x._1)
    val addr = input_rdd_without_headers.map(x => (x._2))
    
//    addr.flatMap(l => l.split(",")).foreach(println)
    val result_rdd = input_rdd_without_headers.flatMap({
      // checking if both key value pair are not nil
      case(key, value) => {
        val elements = value.split(",")
        elements.map(i => (key, i))
      }
    })
    result_rdd.foreach(println)
  }
}