package edu.wm.lxu08

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer
import java.io._

object Question2 {
  
  type Data= (Int, Array[Int]) 
  /** Converts a line of raw input into (user, connections) */
  def convertToData(line: String): Data = {
    
    // Split up the line into fields
    val fields = line.split("\t")  
    
    // Extract the UserID from the first field
    val user = fields(0).toInt
    val test = fields(0)+"\t"
    val nofriend=Array(-1)
    if (test==line){
      return (user, nofriend)
    }
    else{
      
    //Split up the second fields into elements
      val elements = fields(1).split(",")
    
    //Extract user's connections into the connections array
      var connections: ArrayBuffer[Int] = ArrayBuffer()
      for ( connection <- 0 to (elements.length - 1)) {
        connections += elements(connection).toInt
      }
     
    return (user, connections.toArray)
    }
  }
  
  def loaddata(sc:SparkContext): RDD[Data] = {
    val inputFile = sc.textFile("../input-f.txt")
    return inputFile.map(convertToData)
  }

  type pair=((Int,Int),Int)
  // Find each pair of Users who have Cofriend
  def MakePairs(data:Data):Array[pair]={
    val Cofriend=data._1
    val friends=data._2
    var result: ArrayBuffer[pair] = ArrayBuffer()
    if (friends!=Array(-1)){
      for (user1 <-friends){
        for (user2<-friends){
          val onepair=((user1,user2),Cofriend)
          result+= onepair
        }
      }
    }
    return (result.toArray)
  }  
  
  // To filter recommended person who is already a friend of User, make a reference RDD where the numOfCofriend=-999
  def AlreadyFriend(data:Data):Array[pair]={
    val user=data._1
    val friends=data._2
    var result: ArrayBuffer[pair] = ArrayBuffer()
    if (friends!=Array(-1)){
      for (friend<-friends){
        val friendpair=((user, friend),-999)
        result+=friendpair
      }
    }
    return (result.toArray)
  }
  
  def FilterDuplicates(data:pair):Boolean = {
      val userpair = data._1
      val user1=userpair._1
      val user2=userpair._2
      val cofriend= data._2
      
      return (user1!=user2)
  }
  
  // Filter rows where NumOfCofriends<0
  def FilterAlreadyFriend(data:pair):Boolean={
    val index=data._2
    return (index>0)
  }
    
  // Get the top10(or less)
  def Top10(data:Data):Data={
    val user=data._1
    val recommends=data._2
      
    if (recommends.length<=10){
      return data
    }else{
      val top10=recommends.take(10)
      return (user,top10)
    }    
  }
    
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Question2") with Serializable
    
    // Load data
    var data = loaddata(sc)
    
    // Store the users without friends in NullLists
    //val NullLists=data.filter(NoFriends)
    
    // Make (User, Array(Friends)) to ((User1, User2),Array(Cofriends))
    val FriendPairs=data.flatMap(MakePairs)
    
    // Get the RDD of AlreadyFriendPairs for later filter
    val AlreadyFriendPairs=data.flatMap(AlreadyFriend)
    
    // Filter Duplicates for both two RDD
    val FilteredPairs=FriendPairs.filter(FilterDuplicates)
    val FilteredAlreadyFriendPairs=AlreadyFriendPairs.filter(FilterDuplicates)
    
    // Count the number of mutual friends for each pair:((User1,User2),NumOfCofriend)
    val CountOfCofriends=FilteredPairs.mapValues(x => 1).reduceByKey( (x,y) => (x+y))
    
    // Union two RDD to make a big RDD
    val UnionedRDD=CountOfCofriends++FilteredAlreadyFriendPairs
    
    // Group by key and then add up the values so that the pairs which are already friends get values<0
    val Addedup=UnionedRDD.mapValues(x => x).reduceByKey( (x,y) => (x+y))
    
    // Filter the pairs of already friends
    val NewFriends=Addedup.filter(FilterAlreadyFriend)
    
    // Sort each line according to (User1[Ascending]->NumOfCofriend[Descending]->User2[Ascending])
    val Sorted=NewFriends.sortBy(x=>(x._1._1,-x._2,x._1._2))
    
    // Delete the column of NumOfCofriend: (User1, User2)
    val NewPairs=Sorted.map(x=>(x._1))
    
    // Group by User1: (User1, Array(RecommenedFriends))
    val Recommendation=NewPairs.groupByKey().mapValues(f=>f.toArray)

    // Get Top10 for each User
    val TopRecommendation=Recommendation.map(Top10)
    
    // Get the users that can not be recommended
    // That may caused by:1)No friend, 2)Only one friend, 3)All mutual friends are already friends
    val Users=TopRecommendation.map(x=>x._1)
    val AllUsers=data.map(x=>x._1)
    val diff=AllUsers.subtract(Users)
    val NoRecommendation=diff.map(x=>(x,Array(-1)))
    
    // Combine with RDD of Users without friends
    val Combined=TopRecommendation++NoRecommendation
    
    // Sort again
    val Result=Combined.sortByKey().mapValues(f=>f.toList)
    
    // For better output, convert the list to strings
    val Nicer=Result.map(x=>x._1.toString+"\t"+x._2.mkString(","))
    
    val output=Nicer.collect()
    
    output.foreach(print)
    
    // Export to "Question2.txt"
    val file = "Question2.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))

    for (x <- output) {
      writer.write(x + "\n")
    }
    writer.close()    
  }
  
}