/**
 * Reading CSV
 * -- finding average rating of movies from movielens dataset
 * -- dataset available on kaggle
 * 
 * @author deepjyotiroy079
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Reading CSV file").master("local").getOrCreate()
    
    val csv_rdd = spark.sparkContext.textFile("src/resources/u.data")
    val ratings: RDD[(Int, Int)] = csv_rdd.map({
      line => {
        val col = line.split("\t")
        (col(1).toInt, col(2).toInt)
      }
    })
    
    val ratings_per_movie_id = ratings.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avg_ratings = ratings_per_movie_id.map(x => (x._1, (x._2._1 / x._2._2.toFloat))).take(10).foreach(println)
  }
}