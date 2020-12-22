package com.clearscore

import com.clearscore.etl.{AverageCreditScore, EnrichmentBankData, LatestReportPerUsers, UsersByCreditScoreRange, UsersCountByEmploymentStatus}
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.ScallopConf

object Run extends App {

  class Config(arguments: Seq[String]) extends ScallopConf(arguments) {
    val reportName = opt[String](default = Some(""),  required = false)
    val reportsInput = opt[String](default = Some("src/main/resources/bulk-reports/reports/*/*/*/*.json"), required = false)
    val accountsInput = opt[String](default = Some("src/main/resources/bulk-reports/accounts/*.json"), required = false)
    val output = opt[String](default = Some("result"), required = false)
    verify()
  }

  val conf = new Config(args)
  private val reportName = conf.reportName.apply()
  private val reportsInput = conf.reportsInput.apply()
  private val accountsInput = conf.accountsInput.apply()
  private val output = conf.output.apply()

  implicit val spark = SparkSession
    .builder
    .appName(reportName)
    .master("local[*]")
    .getOrCreate()

  private val latestReportsDir = s"$output/LatestReportPerUsers"
  private val outputInsightDir = s"$output/$reportName"

  reportName match {
    case "AverageCreditScore" =>
      AverageCreditScore.run(reportsInput, outputInsightDir)
    case "UsersCountByEmploymentStatus" =>
      UsersCountByEmploymentStatus.run(accountsInput, outputInsightDir)
    case "UsersByCreditScoreRange" =>
      LatestReportPerUsers.run(reportsInput, latestReportsDir)
      UsersByCreditScoreRange.run(latestReportsDir, outputInsightDir)
    case "EnrichmentBankData" =>
      LatestReportPerUsers.run(reportsInput, latestReportsDir)
      EnrichmentBankData.run(latestReportsDir, accountsInput, outputInsightDir)
    case _ =>
      LatestReportPerUsers.run(reportsInput, latestReportsDir)
      AverageCreditScore.run(reportsInput, s"$output/AverageCreditScore")
      UsersCountByEmploymentStatus.run(accountsInput, s"$output/UsersCountByEmploymentStatus")
      UsersByCreditScoreRange.run(latestReportsDir, s"$output/UsersByCreditScoreRange")
      EnrichmentBankData.run(latestReportsDir, accountsInput, s"$output/EnrichmentBankData")
  }

  spark.close()

}
