package main

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Transformations1 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession.builder().appName("Transformation 1").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val data = spark.sparkContext.parallelize(Array(1, 2, 3))
  
    /**
     * map transformations only map the items but doesnot flattens it
     */
    val mapRdd1 = data.map(x=>x.to(4))
    println("Map Rdd")
    for (x <- mapRdd1) {
      println(x)
    }
    
    /**
     * flat map flattens as well as maps the items
     */
    val flatMapRdd1 = data.flatMap(x=>x.to(4))
    println("Flat Map Rdd")
    for(x <- flatMapRdd1){
      println(x)
    }
    
    val rdd1 = spark.sparkContext.parallelize(Array(1, 2, 3, 4))
    val rdd2 = spark.sparkContext.parallelize(Array(4, 5, 6, 7))
    
    /**
     * Union Transformation combines two rdds and flattens them
     */
    val unionRdd = rdd1.union(rdd2)
    println("Union Transformations : ")
    for(x <- unionRdd) {
      println(x)
    }
  }
}