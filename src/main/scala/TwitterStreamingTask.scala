package org.task.twitter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

import java.sql.Timestamp

object TwitterStreamingTask {

  case class Tweet(text: String, time: Timestamp, country: String, placeName: String)

  def setupLogging(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  def mapToTweet(record: Status): Tweet = {

    val text = record.getText
    val time = new java.sql.Timestamp(record.getCreatedAt.getTime())
    val country = record.getPlace.getCountry
    val placeName = record.getPlace.getFullName
    Tweet(text, time, country, placeName)
  }

  def setupTwitter(lines:Array[String]): Unit = {

    for (line <- lines) {
      println(line)
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  def main(args: Array[String]) {

    //Path to work folder
    val path = ""

    //Twitter credentials
    val tweetCredentials = ""


    val sparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("TwitterStreamingTask")
      .getOrCreate()
    setupTwitter(sparkSession.sparkContext.textFile(tweetCredentials).toLocalIterator.toArray)

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))

    setupLogging()

    val tweets = TwitterUtils.createStream(ssc, None)

    val usTweets = tweets.filter(status =>
      status.getPlace != null
    )

    val output = usTweets.map(record => mapToTweet(record))

    output.foreachRDD(rdd => {
        val ds = rdd.toDS()
        ds.write.format("parquet").mode("append").parquet(path+"tweets")
    })

    ssc.checkpoint(path +"checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

}
