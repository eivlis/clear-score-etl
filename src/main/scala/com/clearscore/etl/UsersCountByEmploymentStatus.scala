package com.clearscore.etl

import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{DataFrame, SparkSession}

object UsersCountByEmploymentStatus {

  def run(input: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val usersDS: DataFrame = spark.read.json(input)
      .select($"uuid", $"account.user.employmentStatus")

    val usersCountByEmploymentStatus = usersDS.groupBy("employmentStatus")
      .agg(countDistinct("uuid"))

    usersCountByEmploymentStatus
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(output)
  }

}
