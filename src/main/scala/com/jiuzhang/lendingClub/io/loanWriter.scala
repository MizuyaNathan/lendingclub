package com.jiuzhang.lendingClub.io

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

trait loanWriter extends Logging {

  def writeData(outputDataframe: DataFrame, outputPath: String): Unit = {
    outputDataframe.repartition(1).write.mode(SaveMode.Overwrite).json(outputPath)
  }

}
