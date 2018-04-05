package edu.wm.lxu08.Assignment3

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io._


object Question1 {
   /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    
     val lines = Source.fromFile("../ml-100k/u.item").getLines()
     for (line <- lines) {
       var fields = line.split('|')
       if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
       }
     }
    
     return movieNames
  }
  
  // Case class so we can get column names for our movie ID and ratings
  case class Movie(movieID: Int, ratings: Float)
  
  def mapper(line:String): Movie = {
    val fields = line.split('\t')  
    
    val movies:Movie = Movie(fields(1).toInt, fields(2).toFloat)
    return movies
  }
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Assignment3_Q1")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    
    // Input data
    val lines = spark.sparkContext.textFile("../ml-100k/u.data")
    val movie=lines.map(mapper)

    // Convert to a DataSet
    import spark.implicits._
    val schemaMovie=movie.toDS
    schemaMovie.createOrReplaceTempView("movie")
    
    // Use SQL command to do all the manipulation
    val result = spark.sql("SELECT movieID, AVG(ratings) AS average_rating, COUNT(ratings) AS number_of_ratings FROM movie GROUP BY movieID HAVING number_of_ratings>=100 ORDER BY average_rating ASC")
    val output = result.collect()
    
    val names = loadMovieNames()
    
//    println
//    for (results <- output) {
//      // result is just a Row at this point; we need to cast it back.
//      // Each row has movieID, count as above.
//      println (names(results(0).asInstanceOf[Int]) + ": " + results(1))
//    }
    
    // Output into text file
    val file = "Assignment3_Question1.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- output) {
        writer.write(names(x(0).asInstanceOf[Int]) + ":" + x(1)+ "," + x(2) + "\n")
                    }
    writer.close()    
    
    spark.stop()
  }
}