package com.myrecsys.offline.Spark.featureeng

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType
import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import  redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams
import spire.std.long

object FeatureEngineering {
    val redisEndpoint = "localhost"
    val redisPort = 6379

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("Feature Enginerring")
            .master("local[6]")
            .getOrCreate()
        
        val movieDataPath = this.getClass().getResource("/webroot/data/updated_movies.csv")
        val movieData = spark.read.option("header", "true").csv(movieDataPath.getPath())
        
        val ratingDataPath = this.getClass().getResource("/webroot/data/ratings.csv")
        val ratingData = spark.read.option("header", "true").csv(ratingDataPath.getPath())
        val ratingSamples = ratingData.sample(false, 0.1)

        val samplesWithLabel = addLabel(ratingSamples)
        val samplesWithMoiveFeatures = addMovieFeatures(movieData, samplesWithLabel)
        val samplesWithUserFeatures = addUserFeatures(samplesWithMoiveFeatures)

        splitAndSaveTrainingTestSamples(samplesWithUserFeatures, "/webroot/data")

        //saveMovieFeatures2Redis(samplesWithUserFeatures)
        //saveUserFeatures2Redis(samplesWithUserFeatures)

        spark.close()
    }

    def splitAndSaveTrainingTestSamples(samples: DataFrame, savePath: String): Unit = {
        val Array(training, test) = samples.randomSplit(Array(0.8, 0.2))
        val path = this.getClass().getResource(savePath).getPath()
        training.repartition(1).write.option("header", "true").mode("overwrite").csv(path + "/trainingSamples")
        test.repartition(1).write.option("header", "true").mode("overwrite").csv(path + "/testSamples")
    }

    // スコア3.5以上のものを正例とする
    def addLabel(ratingData: DataFrame): DataFrame = {
        ratingData.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
    }

    // サンプル(レビュー)に映画関連の特徴量を追加する
    def addMovieFeatures(movieData: DataFrame, ratingData: DataFrame): DataFrame = {
        val joinedData = ratingData.join(movieData, Seq("movieId"), "left")

        val extractReleaseYearUdf = udf((rawtitle: String) => {
            val title = rawtitle.trim()
            val pattern = "\\(\\d{4}\\)$".r
            if (title == null || title.length() < 6 || !pattern.findFirstIn(title).isDefined) {
                1990
            } else {
                val yearString = title.substring(title.length() - 5, title.length() - 1)
                yearString.toInt
            }
        })
        // リリース年を追加
        val df1 = joinedData.withColumn("releaseYear", extractReleaseYearUdf(col("title")))
            .drop("title")

        // 映画のジャンルを最大３つまで追加
        val df2 = df1.withColumn("movieGenre1", split(col("genres"), "\\|").getItem(0))
            .withColumn("movieGenre2", split(col("genres"), "\\|").getItem(1))
            .withColumn("movieGenre3", split((col("genres")), "\\|").getItem(2))
        
        // 映画ごとのレビュー数、平均スコア、スコアの標準偏差を持つDataFrame
        val movieRatingFeatures = df2.groupBy(col("movieId"))
            .agg(count(lit(1)).as("movieRatingCount"),
                format_number(avg(col("rating")), d=2).as("movieAvgRating"),
                stddev(col("rating")).as("movieRatingStddev"))
            .na.fill(0)
            .withColumn("movieRatingStddev", format_number(col("movieRatingStddev"), d=2))
        
        val df3 = df2.join(movieRatingFeatures, Seq("movieId"), "left")
        df3
    }

    // サンプル(レビュー)にユーザ関連の特徴量を追加する
    def addUserFeatures(ratingData: DataFrame) = {
        // ユーザが最もよく見る映画のジャンルを抽出UDF
        val extractGenresUdf = udf((genreArray: Seq[String]) => {
            val genreMap = mutable.Map[String, Int]()
            for (element <- genreArray) {
                val genres = element.split("\\|")
                for (genre <- genres) {
                    genreMap(genre) = genreMap.getOrElse[Int](key=genre, default=0) + 1
                }
            }
            val sortedGenres = genreMap.toList.sortBy(_._2)(Ordering.Int.reverse)
            sortedGenres.map(_._1).toSeq
        })
        
        val df = ratingData
            // timestamp時点のユーザのレビューの平均スコア, 標準偏差
            .withColumn("userAvgRating", avg(col("rating")).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)))
            .withColumn("userRatingStddev", stddev(col("rating")).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100,-1)))
            // timestamp時点のユーザがレビューした映画の平均リリース年、標準偏差
            .withColumn("userAvgReleaseYear", avg(col("releaseYear")).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)).cast(IntegerType))
            .withColumn("userReleaseYearStddev", stddev(col("releaseYear")).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)))
            // timestamp時点のユーザのレビュー数
            .withColumn("userRatingCount", count(lit(1)).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1)))
            .na.fill(0)
            // format_numberはString型のColを返す
            .withColumn("userAvgRating", format_number(col("userAvgRating"),d=2))
            .withColumn("userRatingStddev", format_number(col("userRatingStddev"), d=2))
            .withColumn("userReleaseYearStddev", format_number(col("userReleaseYearStddev"), d=2))
            // timestamp時点のユーザが最もよく見る映画のジャンルを最大5つ追加
            .withColumn("userGenres", extractGenresUdf(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null))).over(Window.partitionBy("userId").orderBy("timestamp").rowsBetween(-100, -1))))
            .withColumn("userGenre1", col("userGenres").getItem(0))
            .withColumn("userGenre2", col("userGenres").getItem(1))
            .withColumn("userGenre3", col("userGenres").getItem(2))
            .withColumn("userGenre4", col("userGenres").getItem(3))
            .withColumn("userGenre5", col("userGenres").getItem(5))
            .drop("genres", "userGenres")    
        df
    }

    def saveMovieFeatures2Redis(samples: DataFrame) = {
        val movieSamples = samples.withColumn("movieRowNum", row_number().over(Window.partitionBy("movieId").orderBy("timestamp")))
            .filter(col("movieRowNum") === 1)
            .select("movieId", "releaseYear", "movieGenre1", "movieGenre2", "movieGenre3", "movieRatingCount", "movieAvgRating", "movieRatingStddev")
            .na.fill("")
        movieSamples.printSchema()
        movieSamples.show(100, truncate = false)

        val movieFeaturesPrefix = "mf:"
        val redisClient = new Jedis(redisEndpoint, redisPort)
        //val params = SetParams.setParams().ex(30 * 24 * 60 * 60)
        
        val movieArray = movieSamples.collect()
        var insertedMovieNum = 0
        val movieCount = movieArray.length
        for (movie <- movieArray) {
            val movieKey = movieFeaturesPrefix + movie.getAs[String]("movieId")
            val valueMap = mutable.Map[String, String]()
            valueMap("movieGenre1") = movie.getAs[String]("movieGenre1")
            valueMap("movieGenre2") = movie.getAs[String]("movieGenre2")
            valueMap("movieGenre3") = movie.getAs[String]("movieGenre3")
            valueMap("movieRatingCount") = movie.getAs[Long]("movieRatingCount").toString()
            valueMap("movieAvgRating") = movie.getAs[String]("movieAvgRating")
            valueMap("movieRatingStddev") = movie.getAs[String]("movieRatingStddev")
            valueMap("releaseYear") = movie.getAs[Int]("releaseYear").toString()

            redisClient.hset(movieKey, valueMap.asJava)
            insertedMovieNum += 1
            if (insertedMovieNum % 100 == 0) {
                println(insertedMovieNum + "/" + movieCount + "...")
            }
            redisClient.close()
        }
    }

    def saveUserFeatures2Redis(samples: DataFrame) = {
        val userSamples = samples.withColumn("UserRowNum", row_number().over(Window.partitionBy("userId").orderBy(col("timestamp").desc)))
            .filter(col("userRowNum") === 1)
            .select("userId", "userRatingCount","userAvgReleaseYear","userReleaseYearStddev","userAvgRating","userRatingStddev",
                "userGenre1","userGenre2","userGenre3","userGenre4","userGenre5")
            .na.fill("")
        userSamples.printSchema()
        userSamples.show(100, truncate = false)

        val userFeaturesPrefix = "uf:"
        val redisClient = new Jedis(redisEndpoint, redisPort)
        //val params = SetParams.setParams().ex(30 * 24 * 60 * 60)

        val userArray = userSamples.collect()
        var insertedUserNum = 0
        val userCount = userArray.length
        for (user <- userArray) {
            val userKey = userFeaturesPrefix + user.getAs[String]("userId")
            val valueMap = mutable.Map[String, String]()
            valueMap("userGenre1") = user.getAs[String]("userGenre1")
            valueMap("userGenre2") = user.getAs[String]("userGenre2")
            valueMap("userGenre3") = user.getAs[String]("userGenre3")
            valueMap("userGenre4") = user.getAs[String]("userGenre4")
            valueMap("userGenre5") = user.getAs[String]("userGenre5")
            valueMap("userRatingCount") = user.getAs[Long]("userRatingCount").toString
            valueMap("userAvgReleaseYear") = user.getAs[Int]("userAvgReleaseYear").toString
            valueMap("userReleaseYearStddev") = user.getAs[String]("userReleaseYearStddev")
            valueMap("userAvgRating") = user.getAs[String]("userAvgRating")
            valueMap("userRatingStddev") = user.getAs[String]("userRatingStddev")

            redisClient.hset(userKey, valueMap.asJava)
            insertedUserNum += 1
            if (insertedUserNum % 100 ==0) {
                println(insertedUserNum + "/" + userCount + "...")
            }
        }
        redisClient.close()
    }
}