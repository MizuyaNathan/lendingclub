package com.jiuzhang.lendingClub.io

import com.jiuzhang.lendingClub.`type`.LoanType
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, SparkSession}

trait rejectionReader extends Logging{

  def readRejectionData(inputpath: String, spark: SparkSession): Dataset[LoanType] = {

    import spark.implicits._

    val rawData = spark.read.option("header", "true").csv(inputpath)

    logInfo("reading data from %s".format(inputpath))

    val fields = List("Amount Requested", "Loan Title", "Debt-To-Income Ratio", "State", "Employment Length").map(col)

    rawData.select(fields:_*)
      .withColumnRenamed("Amout Requested", "loan_amnt")
      .withColumnRenamed("Loan Title", "title")
      .withColumnRenamed("Debt-To-Income Ratio", "DTI")
      .withColumnRenamed("State", "addr_state")
      .withColumnRenamed("Employment Length", "emp_length")
      .withColumn("term", lit(null: StringType))
      .withColumn("int_rate", lit(null: StringType))
      .withColumn("installment", lit(null: StringType))
      .withColumn("home_ownership", lit(null: StringType))
      .withColumn("annual_inc", lit(null: StringType))
      .withColumn("has_collection", lit(0))
      .as[LoanType]

  }

}
