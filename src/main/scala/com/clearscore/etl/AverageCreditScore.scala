package com.clearscore.etl

import org.apache.spark.sql.functions.{avg, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Calculates the average credit score over all reports
 */
object AverageCreditScore {

  def run(input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val scoresDS: DataFrame = spark.read.json(input)
      .select(explode($"report.ScoreBlock.Delphi.Score") as "score")

    val averageCreditScoreDF = run(scoresDS);
    averageCreditScoreDF
      .write.mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(output)
  }

  def run(dataFrame: DataFrame): DataFrame = {
    dataFrame.agg(avg("score") as "avg_credit_score")
  }
}
