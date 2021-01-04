package com.jiuzhang.lendingClub.aggregation

import com.jiuzhang.lendingClub.`type`.LoanType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait LoanInfoAggregator extends Logging {

  def loanInfoAggregator(rejectionDs: Dataset[LoanType], loanDs: Dataset[LoanType], spark: SparkSession): DataFrame = {

    import spark.implicits._

    val unionDs = rejectionDs.unionByName(loanDs)

    val aggregatedDf = unionDs.groupBy("term", "home_ownership", "addr_state", "tile", "emp_length")
      .agg(avg($"loan_amnt").as("avg_loan_amnt"),
        avg($"int_rate").as("avg_int_rate"),
        avg($"annual_inc").as("avg_annual_inc"),
        avg($"DTI").as("avg_DTI"),
        sum($"has_collection").as("sum_has_collection"),
        avg($"installment").as("avg_installment")
      )

    aggregatedDf
  }

}
