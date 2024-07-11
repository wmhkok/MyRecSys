package com.myrecsys.offline.Spark.embedding

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.ml.linalg.Vectors
import org.sparkproject.dmg.pmml.True

import java.io.BufferedWriter
import java.io.FileWriter

import java.io.File
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object Embedding {
  val redisEndpoint = "localhost"
  val redisPort = 6379

  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
      .builder()
      .appName("Embedding Model")
      .master("local[6]")
      .getOrCreate()

      val ratingSamplePath = "/webroot/data/ratings.csv"
      val embDim = 10

      //val samples = processItemSequence(spark, ratingSamplePath)
      //val model = trainItem2Vec(spark, samples, embDim, "item2VecEmb.txt", save2Redis=false, "i2vEmb")
      val modelSavePath = this.getClass.getResource("/webroot/modeldata/").getPath() + "word2VecModel"
      //model.save(spark.sparkContext, modelSavePath)
      val model = Word2VecModel.load(spark.sparkContext, modelSavePath)
      generateuserEmb(spark, ratingSamplePath, model, embDim, "userEmb.txt", save2Redis=false, "uEmb")
      
  }

  def processItemSequence(spark: SparkSession, dataPath: String): RDD[Seq[String]] = {
    val path = this.getClass.getResource(dataPath).getPath()
    val ratingSamples = spark.read.option("header", "true").csv(path)

    val sortUdf = udf((rows: Seq[Row]) => {
      rows.map({case Row(movieId: String, timestamp: String) => (movieId, timestamp)})
        .sortBy({case (_, timestamp) => timestamp})
        .map({case (movieId, _) => movieId})
    })

    ratingSamples.printSchema()

    val userSeq = ratingSamples.where(col("rating") >= 3.5)
      .groupBy("userId").agg(sortUdf(collect_list(struct("movieId", "timestamp"))).as("movieIds"))
      .withColumn("movieIdStr", array_join(col("movieIds"), " "))
    
    userSeq.select("userId", "movieIdStr").show(10, truncate = false)
    userSeq.select("movieIdStr").rdd.map(r => r.getAs[String]("movieIdStr").split(" ").toSeq)
  }

  def trainItem2Vec(spark: SparkSession, samples: RDD[Seq[String]], embDim: Int, outputFilename: String, save2Redis:Boolean, redisKeyPrefix: String) = {
    val word2Vec = new Word2Vec()
      .setVectorSize(embDim)
      .setWindowSize(5)
      .setNumIterations(10)
    
    val model = word2Vec.fit(samples)

    val embFolderPath = this.getClass.getResource("/webroot/modeldata/").getPath()
    val file = new File(embFolderPath + outputFilename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (movieId <- model.getVectors.keys) {
      bw.write(movieId + ":" + model.getVectors(movieId).mkString(" ") + "\n")
    }
    bw.close()

    if (save2Redis) {
      val redisClient = new Jedis(redisEndpoint, redisPort)
      val DFWithBucketId = embedingLSH(spark, model.getVectors)
      //DFWithBucketId.foreach()
      for (movieId <- model.getVectors.keys) {
        redisClient.set(redisKeyPrefix + ":" + movieId,  model.getVectors(movieId).mkString(" "))
      }
      redisClient.close()
    }
  
    
    model
  }

  def embedingLSH(spark: SparkSession, movieEmbMap: Map[String, Array[Float]]) = {
     // LSHモデルはdense vector型を受け取る
    val movieEmbSeq = movieEmbMap.toSeq.map(item => (item._1, Vectors.dense(item._2.map(f => f.toDouble))))
    val movieEmbDF = spark.createDataFrame(movieEmbSeq).toDF("movieId", "emb")

    val bucketProjectionLSH = new BucketedRandomProjectionLSH()
      .setBucketLength(0.1)
      .setNumHashTables(3)
      .setInputCol("emb")
      .setOutputCol("bucketId")

    val model = bucketProjectionLSH.fit(movieEmbDF)
    val updatedDF = model.transform(movieEmbDF)
    updatedDF
  }

  // ユーザが高評価した映画のembeddingの平均をそのユーザのembeddingとする
  def generateuserEmb(spark: SparkSession, dataPath: String, word2VecModel: Word2VecModel, embDim: Int, outputFilename: String, save2Redis:Boolean, redisKeyPrefix:String) = {
    val path = this.getClass.getResource(dataPath).getPath()
    val ratingSamples = spark.read.option("header", "true").csv(path)
    ratingSamples.show(10, false)
    val userGroupedDF = ratingSamples.filter(col("rating") >= 3.5).groupBy("userId").agg(collect_list("movieId").as("reviewedMovies"))

    val userEmbeddings = userGroupedDF.rdd.map(row => {
      val userId = row.getAs[String]("userId")
      val movieIds = row.getAs[mutable.ArraySeq[String]]("reviewedMovies")
      println("Generating user " + userId + "'s embedding now...") // 162541 users in total
      var movieCount = 0
      val userEmb = movieIds.foldLeft(Array.fill[Float](embDim)(0.0f))((summedEmb, movieId) => {
        movieCount += 1
        word2VecModel.getVectors.get(movieId) match {
          case Some(movieEmb) =>
            summedEmb.zip(movieEmb).map { case (x,y) => x + y} 
          case None => summedEmb
        }
      }).map(x => x / movieCount)
      (userId, userEmb)
    }).collect()
    println("user embedding generation complete")

    val embFolderPath = this.getClass.getResource("/webroot/modeldata/")
    val file = new File(embFolderPath.getPath + outputFilename)
    val bw = new BufferedWriter(new FileWriter(file))

    println("start writing out the file")
    userEmbeddings.foreach(userEmbedding => {
      val (userId, userEmb) = userEmbedding
      bw.write(userId + ":" + userEmb.mkString(" ") + "\n")
    })
    bw.close()

    if (save2Redis) {
      val redisClient = new Jedis(redisEndpoint, redisPort)
      val params = SetParams.setParams()
      for (userEmb <- userEmbeddings) {
        redisClient.set(redisKeyPrefix + ":" + userEmb._1, userEmb._2.mkString(" "), params)
      }
      redisClient.close()
    }
  }
}