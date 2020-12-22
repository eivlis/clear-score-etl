package com.clearscore.etl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, row_number, to_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object LatestReportPerUsers {

  def run(input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val reportsDS: DataFrame = spark.read.json(input)
      .withColumn("pulled-timestamp", to_timestamp($"pulled-timestamp"))

    val window = Window.partitionBy("user-uuid").orderBy(desc("pulled-timestamp"))

    val latestReportPerUser = reportsDS.withColumn("row", row_number.over(window))
      .where($"row" === 1).drop("row")

    latestReportPerUser
      .write.mode("overwrite")
      .json(output)
  }
}
