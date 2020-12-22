package com.clearscore.etl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{ceil, explode, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UsersByCreditScoreRange {

  def run(input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val rangeSplit = 50

    val scoresDF = spark.read.json(input)
      .select($"user-uuid", explode($"report.ScoreBlock.Delphi.Score") as "score")


    val usersByCreditScoreRange = scoresDF
      .withColumn("range_category", ceil($"score" / rangeSplit))
      .groupBy("range_category").count()
      .withColumn("score_range", formatRange($"range_category", lit(rangeSplit)))
      .select($"score_range", $"count")

    usersByCreditScoreRange
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(output)
  }

  def formatRange: UserDefinedFunction = {
    udf((score: Int, range: Int) => {
      val upperBound = if (score == 0) range else score * range
      val lowerBound = if (upperBound == range) 0 else (upperBound - range) + 1
      s"$lowerBound-$upperBound"
    })
  }
}
