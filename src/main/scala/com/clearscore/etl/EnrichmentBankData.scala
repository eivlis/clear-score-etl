package com.clearscore.etl

import org.apache.spark.sql.SparkSession

object EnrichmentBankData {

  def run(inputReport: String, inputAccount: String, output: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val reportDF = spark.read.json(inputReport)
      .select(
        $"user-uuid",
        $"report.Summary.Payment_Profiles.CPA.Bank.Total_number_of_Bank_Active_accounts_" as "total_active_accounts",
        $"report.Summary.Payment_Profiles.CPA.Bank.Total_outstanding_balance_on_Bank_active_accounts" as "total_outstanding_balance"
      )

    val accountDF = spark.read.json(inputAccount).select(
      $"uuid",
      $"account.user.employmentStatus",
      $"account.user.bankName"
    )

    val summary = accountDF.join(reportDF, accountDF("uuid") === reportDF("user-uuid"), "inner")

    summary
      .drop($"user-uuid")
      .coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(output)
  }
}
