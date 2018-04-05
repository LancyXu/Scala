package edu.wm.lxu08

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._ //only print out error level messages
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object SimpleSelection {
  
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
  
  def parseLine(line: String) = {
      // Split by \t
      val fields = line.split("\t")
      // Extract the movieID and ratings fields, and convert to integers
      val movieID = fields(1).toInt
      val ratings = fields(2).toDouble
      // Create a tuple that is our result.
      (movieID, ratings)
  }
  
  //Filter movies with less than 100 ratings
  type MovieRatingPair = (Int, (Double, Int))
  def filter100(movieRatings:MovieRatingPair):Boolean = {
    val totalRatings = movieRatings._2._2
    return totalRatings >= 100
  }
  
  //Flip over averageRatings with movieID to sort
  def flipover(movieRatings:(Int, (Double, Int)))={
    val movieID=movieRatings._1
    val averageRatings=movieRatings._2._1
    val countOfRatings=movieRatings._2._2
    
    (averageRatings,(movieID, countOfRatings))
  }
 
  /** Our main function where the work is occurring */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine, named Question1
    val sc = new SparkContext("local[*]", "Question1")
   
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")

    // Use our parseLines function to convert to (movieID, ratings) tuples
    val rdd = lines.map(parseLine)
    
    
    // Sum up the ratings and count
    val movieRatings = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))
    // So now we have tuples of (movie, (sumOfRatings, countOfRatings))
    
    //Filter movies with less than 100 ratings
    val afterfilter = movieRatings.filter(filter100)
    
    // To compute the average we divide sumOfRatings / countOfRatings for each movie.
    val averagesByMovie = afterfilter.mapValues(x => (x._1 / x._2, x._2))
    
    // Flip (movie, (averageRatings, countOfRatings)) tuples to (averageRatings,(movie, countOfRatings)) and then sort by key (averageRatings)
    val moviesorted = averagesByMovie.map(flipover).sortByKey()
    
    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = moviesorted.map(x  => (nameDict.value(x._2._1), (x._1, x._2._2)))
    
    val results = sortedMoviesWithNames.collect()
    results.foreach(println)
    
    val file = "SimpleSelection.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
                    }
    writer.close()    
  }
}
