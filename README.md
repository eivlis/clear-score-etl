# ClearScore Data Engineering Tech Test
This project provides data aggregations and insights for ClearScore using some data from South African users.

## Create reports for:
1. Average Credit Score
2. Number of users grouped by their employment status
3. Number of users in score ranges
4. Enriched bank data

## Run the reports via sbt

For compiling the code and running the unit tests:

```bash
sbt compile
```

Run reports: 
```bash
sbt run
param:
--reportName [AverageCreditScore, UsersByCreditScoreRange, UsersCountByEmployentStatus, EnrichmentBankData]
    default: run all
--reportsInput 
    default: src/main/resources/bulk-reports/reports/*/*/*/*.json
--accountsInput
    default: src/main/resources/bulk-reports/accounts/*.json
--output
    default: result


sbt run --reportName AverageCreditScore --output etl
```




