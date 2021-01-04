package com.jiuzhang.lendingClub

import com.jiuzhang.lendingClub.aggregation.LoanInfoAggregator
import com.jiuzhang.lendingClub.io.{loanReader, loanWriter, rejectionReader}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object analyze extends Logging with loanReader with rejectionReader with LoanInfoAggregator with loanWriter{

  def main (args: Array[String]): Unit = {
    if (args.length != 3) {
      logError("This program is expecting 3 parameters")
    }

    val spark = SparkSession.builder.appName("loan").getOrCreate()

    val loaninputPath = args(0)
    val rejectionInputPath = args(1)
    val outputPath = args(2)

    val loanDs = readLoanData(loaninputPath, spark)
    val rejectionDs = readRejectionData(rejectionInputPath, spark)
    val aggDf = loanInfoAggregator(loanDs, rejectionDs, spark)
    writeData(aggDf, outputPath)
  }

}
